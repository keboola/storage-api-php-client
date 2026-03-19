<?php

declare(strict_types=1);

namespace Keboola\StorageApi;

class RefreshFileCredentialsWrapper
{
    private Client $client;
    private int $fileId;

    public function __construct(Client $client, int $fileId)
    {
        $this->client = $client;
        $this->fileId = $fileId;
    }

    public function refreshCredentials(): array
    {
        $preparedFileResult = $this->client->refreshFileCredentials($this->fileId);
        $uploadParams = $preparedFileResult['gcsUploadParams'];
        return [
            'credentials' => [
                'access_token' => $uploadParams['access_token'],
                'expires_in' => $uploadParams['expires_in'],
                'token_type' => $uploadParams['token_type'],
            ],
            'projectId' => $uploadParams['projectId'],
        ];
    }
}
