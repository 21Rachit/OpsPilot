# TLS certificate expired

Use this runbook when clients cannot connect over HTTPS because a certificate has expired
or is about to. Browsers show a security warning and API clients fail their requests.

## Symptoms

- Browsers show "Your connection is not private" or NET::ERR_CERT_DATE_INVALID
- API clients fail with "x509: certificate has expired or is not yet valid"
- Alerts such as "certificate expires in less than 7 days"

## Diagnose

Check the dates on the certificate the server is actually sending:

```bash
echo | openssl s_client -connect api.example.com:443 -servername api.example.com 2>/dev/null \
  | openssl x509 -noout -dates
```

If `notAfter` is in the past, the certificate has expired. If the new certificate exists
but the old one is still served, the server was not reloaded after renewal.

## Fix

1. Renew the certificate. With cert-manager, delete the certificate's secret and it is
   issued again within a minute. With certbot, run `sudo certbot renew`.
2. Reload the web server or load balancer, so it serves the new certificate.
3. Run the openssl command again and confirm `notAfter` is in the future.

## Escalate

If renewal fails, for example because DNS validation fails, page the platform on-call team.
