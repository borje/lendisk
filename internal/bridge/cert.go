package bridge

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"

	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"time"
)

// The generated endpoint certificate and its key, in the config
// directory (EP-17, SEC-3).
const (
	CertFile = "endpoint.crt"
	KeyFile  = "endpoint.key"
)

// ReadCert reads the generated certificate from the config directory.
func ReadCert(dir string) (*x509.Certificate, error) {
	b, err := os.ReadFile(filepath.Join(dir, CertFile))
	if err != nil {
		return nil, err
	}
	blk, _ := pem.Decode(b)
	if blk == nil || blk.Type != "CERTIFICATE" {
		return nil, fmt.Errorf("%s: no PEM certificate", filepath.Join(dir, CertFile))
	}
	return x509.ParseCertificate(blk.Bytes)
}

// Fingerprint is the SHA-256 of a certificate's DER form, as
// colon-separated hex pairs (the form `openssl x509 -fingerprint
// -sha256` prints).
func Fingerprint(der []byte) string {
	sum := sha256.Sum256(der)
	parts := make([]string, len(sum))
	for i, b := range sum {
		parts[i] = fmt.Sprintf("%02X", b)
	}
	return strings.Join(parts, ":")
}

// ensureCert returns the generated certificate for exactly these IP
// addresses and DNS names, reusing the one in dir when its SANs match and
// its key loads, and otherwise generating a self-signed ECDSA P-256
// certificate valid ten years. The key is written at 0600 from the first
// byte, before the certificate, so a crash between leaves a pair that
// does not load and is regenerated. A reused key is checked as a
// tls = "files" key is: one others can read, or another user owns, is
// refused (SEC-3, #53).
func ensureCert(dir string, ips []net.IP, names []string) (*tls.Certificate, bool, error) {
	certFile, keyFile := filepath.Join(dir, CertFile), filepath.Join(dir, KeyFile)
	if old, err := ReadCert(dir); err == nil && sameSANs(old, ips, names) {
		if pair, err := tls.LoadX509KeyPair(certFile, keyFile); err == nil {
			if err := checkKeyFile(keyFile); err != nil {
				return nil, false, err
			}
			return &pair, false, nil
		}
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, false, err
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 127))
	if err != nil {
		return nil, false, err
	}
	now := time.Now()
	cn := "lendisk endpoint"
	if len(names) > 0 {
		cn += " " + names[0]
	}
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    now.Add(-time.Hour),
		NotAfter:     now.AddDate(10, 0, 0),
		// Self-signed and its own trust anchor: clients are given this
		// certificate as their CA (restic --cacert).
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:           ips,
		DNSNames:              names,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return nil, false, err
	}
	kder, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return nil, false, err
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: kder})
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	if err := writeAtomic(keyFile, keyPEM, 0o600); err != nil {
		return nil, false, err
	}
	if err := writeAtomic(certFile, certPEM, 0o644); err != nil {
		return nil, false, err
	}
	pair, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, false, err
	}
	return &pair, true, nil
}

func sameSANs(c *x509.Certificate, ips []net.IP, names []string) bool {
	if len(c.IPAddresses) != len(ips) || !slices.Equal(c.DNSNames, names) {
		return false
	}
	for i := range ips {
		if !c.IPAddresses[i].Equal(ips[i]) {
			return false
		}
	}
	return true
}

// writeAtomic writes b to path through a temporary file created at mode
// 0600 and renamed into place.
func writeAtomic(path string, b []byte, mode os.FileMode) error {
	f, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if _, err := f.Write(b); err != nil {
		f.Close()
		return err
	}
	if err := f.Chmod(mode); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(f.Name(), path)
}

// checkKeyFile refuses a TLS key that anyone but the service user can
// read, or that another user owns (SEC-3).
func checkKeyFile(keyFile string) error {
	fi, err := os.Stat(keyFile)
	if err != nil {
		return fmt.Errorf("endpoint TLS key: %w", err)
	}
	if fi.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("endpoint TLS key %s has mode %04o: make it readable by the service user only (chmod 600)", keyFile, fi.Mode().Perm())
	}
	if st, ok := fi.Sys().(*syscall.Stat_t); ok && int(st.Uid) != os.Geteuid() {
		return fmt.Errorf("endpoint TLS key %s belongs to uid %d: chown it to the service user (uid %d)", keyFile, st.Uid, os.Geteuid())
	}
	return nil
}

// loadUserPair loads a tls = "files" pair, refusing a key that anyone but
// the service user can read (SEC-3).
func loadUserPair(certFile, keyFile string) (*tls.Certificate, error) {
	if err := checkKeyFile(keyFile); err != nil {
		return nil, err
	}
	pair, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("endpoint TLS certificate %s and key %s: %w", certFile, keyFile, err)
	}
	return &pair, nil
}
