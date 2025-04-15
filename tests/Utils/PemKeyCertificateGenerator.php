<?php

declare(strict_types=1);

namespace Keboola\Test\Utils;

use OpenSSLCertificateSigningRequest;
use SensitiveParameter;

/**
 * @internal
 * @todo refactor this class to shared lib
 */
class PemKeyCertificateGenerator
{
    public function createPemKeyCertificate(#[SensitiveParameter] string|null $password): PemKeyCertificatePair
    {
        $res = openssl_pkey_new([
            'private_key_bits' => 2048,
            'private_key_type' => OPENSSL_KEYTYPE_RSA,
        ]);
        assert($res !== false, 'Failed to generate a new private key');

        // Extract the private key
        openssl_pkey_export($res, $privateKey, $password);

        // Generate a Certificate Signing Request (CSR)
        $csr = openssl_csr_new((CertificateSigningRequest::createDefault())->toArray(), $res);
        assert($csr instanceof OpenSSLCertificateSigningRequest, 'Failed to generate a new CSR');

        // Self-sign the CSR to create the certificate
        $cert = openssl_csr_sign($csr, null, $res, 365);
        assert($cert !== false, 'Failed to sign the CSR');

        openssl_pkey_export($res, $privateKeyPem);

        // Extraction of public key
        $details = openssl_pkey_get_details($res);
        assert($details !== false, 'Failed to get details of the private key');
        assert(array_key_exists('key', $details) !== false, 'Failed to get the public key from the private key');
        $publicKeyPem = $details['key'];

        return new PemKeyCertificatePair($privateKey, $publicKeyPem);
    }
}
