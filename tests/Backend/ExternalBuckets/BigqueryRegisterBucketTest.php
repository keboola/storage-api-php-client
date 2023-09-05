    /**
     * @return array{
     * type: string,
     * project_id: string,
     * private_key_id: string,
     * private_key: string,
     * client_email: string,
     * client_id: string,
     * auth_uri: string,
     * token_uri: string,
     * auth_provider_x509_cert_url: string,
     * client_x509_cert_url: string,
     * }
     */
    public static function getCredentialsArray(): array
    {
        /**
         * @var array{
         * type: string,
         * project_id: string,
         * private_key_id: string,
         * private_key: string,
         * client_email: string,
         * client_id: string,
         * auth_uri: string,
         * token_uri: string,
         * auth_provider_x509_cert_url: string,
         * client_x509_cert_url: string,
         * } $credentialsArr
         */
        $credentialsArr = (array) json_decode(BQ_KEY_FILE_FOR_EXTERNAL_BUCKET, true, 512, JSON_THROW_ON_ERROR);

        return $credentialsArr;
    }

    /**
     * @param array $externalCredentials
     * @return AnalyticsHubServiceClient
     * @throws \Google\ApiCore\ValidationException
     */
    public function getAnalyticsHubServiceClient(array $externalCredentials): AnalyticsHubServiceClient
    {
        $analyticHubClient = new AnalyticsHubServiceClient([
            'credentials' => $externalCredentials,
        ]);
        return $analyticHubClient;
    }

    /**
     * @param array $externalCredentials
     * @return BigQueryClient
     */
    public function getBigQueryClient(array $externalCredentials): BigQueryClient
    {
        $handler = new BigQueryClientHandler(new Client());

        $bqClient = new BigQueryClient([
            'keyFile' => $externalCredentials,
            'httpHandler' => $handler,
        ]);
        return $bqClient;
    }
}
