package cli

import (
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"

	"github.com/borje/lendisk/internal/bridge"
)

// endpointCmd is `lendisk endpoint`: the S3 endpoint's LAN side (EP-17).
func (a *app) endpointCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "endpoint", Short: "The S3 endpoint for other machines on the LAN"}
	cert := &cobra.Command{
		Use:   "cert",
		Short: "Print the generated certificate's path and SHA-256 fingerprint",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			cfg, _, err := a.load(a.errOut)
			if err != nil {
				return err
			}
			path := filepath.Join(cfg.ConfigDir(), bridge.CertFile)
			c, err := bridge.ReadCert(cfg.ConfigDir())
			if errors.Is(err, fs.ErrNotExist) {
				return fmt.Errorf("no certificate at %s: `lendisk serve` generates it when an [[endpoint.listen]] entry has tls = \"generate\"", path)
			} else if err != nil {
				return err
			}
			var sans []string
			for _, ip := range c.IPAddresses {
				sans = append(sans, ip.String())
			}
			sans = append(sans, c.DNSNames...)
			fmt.Fprintf(a.out, "Certificate: %s\n", path)
			fmt.Fprintf(a.out, "SHA-256:     %s\n", bridge.Fingerprint(c.Raw))
			fmt.Fprintf(a.out, "Names:       %s\n", strings.Join(sans, ", "))
			fmt.Fprintf(a.out, "Valid until: %s\n", c.NotAfter.Format("2006-01-02"))
			return nil
		},
	}
	cmd.AddCommand(cert)
	return cmd
}
