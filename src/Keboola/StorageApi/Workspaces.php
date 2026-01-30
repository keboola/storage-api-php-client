<?php

namespace Keboola\StorageApi;

use Keboola\StorageApi\Workspaces\ResetCredentialsRequest;
use Keboola\StorageApi\Workspaces\SetPublicKeyRequest;

/**
 * @phpstan-type WorkspaceResponse array{
 *      id: int,
 *      type: 'file'|'table',
 *      name: string|null,
 *      component: string|null,
 *      configurationId: string|null,
 *      created: string,
 *      connection: array{
 *       backend: string,
 *       region: string|null,
 *       host: string,
 *       database: string|null,
 *       schema: string|null,
 *       warehouse: string|null,
 *       user: string,
 *       loginType: string,
 *      }|array{
 *       backend: string,
 *       container: string|null,
 *       region: string|null
 *      },
 *      backendSize: string|null,
 *      statementTimeoutSeconds: int,
 *      creatorToken: array{
 *       id: int,
 *       description: string|null
 *      },
 *      readOnlyStorageAccess: bool,
 *      platformUsageType: string|null,
 *  }
 *
 * @phpstan-type CreateWorkspaceOptions array{
 *      backend?: string,
 *      backendSize?: string,
 *      loginType?: WorkspaceLoginType,
 *      networkPolicy?: string,
 *      readOnlyStorageAccess?: bool,
 *  }
 */
class Workspaces
{
    /**
     * @var Client
     */
    private $client;

    public function __construct(Client $client)
    {
        $this->client = $client;
    }

    /**
     * @param CreateWorkspaceOptions $options
     * @return array
     */
    public function createWorkspace(array $options = [], bool $async = false): array
    {
        return $this->decorateWorkspaceCreateWithCredentials(
            $options,
            function (array $options) use ($async) {
                $workspaceResponse = $this->internalCreateWorkspace($async, $options, true);
                assert(is_array($workspaceResponse));

                return $workspaceResponse;
            },
        );
    }

    public function queueCreateWorkspace(array $options = []): int
    {
        $job = $this->internalCreateWorkspace(true, $options, false);
        return (int) $job['id'];
    }

    /**
     * @return WorkspaceResponse[]
     */
    public function listWorkspaces(): array
    {
        $result = $this->client->apiGet('workspaces');
        assert(is_array($result));
        return $result;
    }

    /**
     * @return WorkspaceResponse
     */
    public function getWorkspace(int|string $id): array
    {
        /** @var WorkspaceResponse $result */
        $result = $this->client->apiGet("workspaces/{$id}");
        assert(is_array($result));
        return $result;
    }

    /**
     * @param int $id
     * @param array $options (boolean) async
     * @return void
     */
    public function deleteWorkspace($id, array $options = [], bool $async = false)
    {
        $url = sprintf('workspaces/%s', $id);
        if (!array_key_exists('async', $options)) {
            // to prevent bc use $async argument only if async is not part of options
            $options['async'] = $async;
        }
        $url .= '?' . http_build_query($options);

        $this->client->apiDelete($url);
    }

    /**
     * @param int $id
     * @return int jobId
     */
    public function queueDeleteWorkspace($id)
    {
        $url = sprintf('workspaces/%s', $id);
        $url .= '?' . http_build_query(['async' => true]);
        $job = $this->client->apiDelete($url, false);
        assert(is_array($job));
        assert(array_key_exists('id', $job));
        return (int) $job['id'];
    }

    /**
     * @param int $id
     * @param array $options input[mappings] (required), preserve (optional)
     * @return void
     */
    public function loadWorkspaceData($id, array $options = [])
    {
        $this->client->apiPostJson("workspaces/{$id}/load", $options);
    }

    /**
     * @param int $id
     * @param array $options input[mappings] (required), preserve (optional)
     * @return void
     */
    public function cloneIntoWorkspace($id, array $options = [])
    {
        $this->client->apiPostJson("workspaces/{$id}/load-clone", $options);
    }

    public function resetWorkspacePassword(int|string $id): array
    {
        $result = $this->client->apiPostJson("workspaces/{$id}/password");
        assert(is_array($result));
        return $result;
    }

    /**
     * @param array $options -- required input[mappings], optional preserve
     */
    public function queueWorkspaceLoadData(int $id, array $options = []): int
    {
        /** @var array{id: int} $job */
        $job = $this->client->apiPostJson("workspaces/{$id}/load", $options, false);
        return (int) $job['id'];
    }

    /**
     * @param array $options -- required input[mappings], optional preserve
     */
    public function queueWorkspaceCloneInto(int $id, array $options = []): int
    {
        /** @var array{id: int} $job */
        $job = $this->client->apiPostJson("workspaces/{$id}/load-clone", $options, false);
        return (int) $job['id'];
    }

    /**
     * Queue workspace unload operation
     *
     * @param int $id Workspace ID
     * @param array $options Optional parameters (e.g., only-direct-grants)
     * @return array Array of job IDs
     */
    public function queueUnload(int $id, array $options = []): array
    {
        $url = "workspaces/{$id}/unload";
        if (!empty($options)) {
            $url .= '?' . http_build_query($options);
        }

        $jobs = $this->client->apiPostJson($url, [], false);
        assert(is_array($jobs));

        return array_map(fn($job) => (int) $job['id'], $jobs);
    }

    public function executeQuery(int $id, string $query): array
    {
        $result = $this->client->apiPostJson(
            "workspaces/{$id}/query",
            [
                'query' => $query,
            ],
        );
        assert(is_array($result));

        return $result;
    }

    /**
     * @param CreateWorkspaceOptions $options
     * @param callable(CreateWorkspaceOptions $options): array $createWorkspace
     * @return array
     */
    public function decorateWorkspaceCreateWithCredentials(array $options, callable $createWorkspace): array
    {
        $workspaceResponse = $createWorkspace($options);

        if (($options['loginType'] ?? WorkspaceLoginType::DEFAULT)->isPasswordLogin()) {
            $resetPasswordResponse = $this->resetWorkspacePassword($workspaceResponse['id']);
            $workspaceResponse = Workspaces::addCredentialsToWorkspaceResponse(
                $workspaceResponse,
                $resetPasswordResponse,
            );
        }

        return $workspaceResponse;
    }

    public static function addCredentialsToWorkspaceResponse(
        array $workspaceResponse,
        array $resetPasswordResponse,
    ): array {
        if ($workspaceResponse['type'] === 'file') {
            $secret = 'connectionString';
        } elseif ($workspaceResponse['connection']['backend'] === 'bigquery') {
            $secret = 'credentials';
        } else {
            $secret = 'password';
        }

        unset($workspaceResponse['connection'][$secret]);

        return array_merge_recursive($workspaceResponse, [
            'connection' => [
                $secret => $resetPasswordResponse[$secret],
            ],
        ]);
    }

    private function internalCreateWorkspace(bool $async, array $options, bool $handleAsyncTask): array
    {
        $url = 'workspaces';
        $requestOptions = [Client::REQUEST_OPTION_EXTENDED_TIMEOUT => true];
        if ($async) {
            $url .= '?' . http_build_query(['async' => $async]);
            $requestOptions = [];
        }

        return $this->client->apiPostJson($url, $options, $handleAsyncTask, $requestOptions);
    }

    public function setPublicKey(int|string $workspaceId, SetPublicKeyRequest $data): void
    {
        $url = sprintf('workspaces/%s/public-key', $workspaceId);
        if ($data->keyName !== null) {
            $url .= '/' . $data->keyName->value;
        }
        $this->client->apiPostJson($url, [
            'publicKey' => $data->publicKey,
        ]);
    }

    /**
     * @return array{
     *     password?: string,
     *     credentials?: array<mixed>,
     * }
     */
    public function resetCredentials(
        int|string $workspaceId,
        ResetCredentialsRequest $data,
    ): array {
        $workspaceData = $this->getWorkspace($workspaceId);
        $loginType = WorkspaceLoginType::from(
            $workspaceData['connection']['loginType'] ?? WorkspaceLoginType::DEFAULT->value,
        );

        if ($loginType->isKeyPairLogin()) {
            if ($data->publicKey === null) {
                throw new ClientException(sprintf(
                    'Workspace with login type "%s" requires "publicKey" credentials.',
                    $loginType->value,
                ));
            }

            $this->setPublicKey($workspaceId, new SetPublicKeyRequest(
                publicKey: $data->publicKey,
            ));

            return [];
        }

        if ($loginType->isPasswordLogin()) {
            if ($data->publicKey !== null) {
                throw new ClientException(sprintf(
                    'Workspace with login type "%s" does not support "publicKey" credentials.',
                    $loginType->value,
                ));
            }

            return $this->resetWorkspacePassword($workspaceId);
        }

        throw new ClientException(sprintf(
            'Credentials reset is not supported for workspace with login type "%s".',
            $loginType->value,
        ));
    }

    public function createCredentials(int|string $workspaceId): array
    {
        $result = $this->client->apiPostJson("workspaces/{$workspaceId}/credentials");
        assert(is_array($result));
        return $result;
    }

    public function getCredentials(int|string $workspaceId, int|string $credentialsId): array
    {
        $result = $this->client->apiGet("workspaces/{$workspaceId}/credentials/{$credentialsId}");
        assert(is_array($result));
        return $result;
    }

    public function deleteCredentials(int|string $workspaceId, int|string $credentialsId): void
    {
        $this->client->apiDelete("workspaces/{$workspaceId}/credentials/{$credentialsId}");
    }
}
