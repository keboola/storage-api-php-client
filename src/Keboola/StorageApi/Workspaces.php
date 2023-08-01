<?php
/**
 * Created by JetBrains PhpStorm.
 * User: martinhalamicek
 * Date: 16/09/14
 * Time: 01:48
 * To change this template use File | Settings | File Templates.
 */

namespace Keboola\StorageApi;

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
     * @param array $options backend (optional)
     * @return array
     */
    public function createWorkspace(array $options = [], bool $async = false)
    {
        $url = 'workspaces';
        $requestOptions = [Client::REQUEST_OPTION_EXTENDED_TIMEOUT => true];
        if ($async) {
            $url .= '?' . http_build_query(['async' => $async]);
            $requestOptions = [];
        }

        $result = $this->client->apiPostJson($url, $options, true, $requestOptions);
        assert(is_array($result));

        $response = $this->resetWorkspacePassword($result['id']);
        if ($result['type'] === 'file') {
            $secret = 'connectionString';
        } elseif ($result['connection']['backend'] === 'bigquery') {
            $secret = 'credentials';
        } else {
            $secret = 'password';
        }

        return array_merge_recursive($result, [
            'connection' => [
                $secret => $response[$secret],
            ],
        ]);
    }

    /**
     * @return array
     */
    public function listWorkspaces()
    {
        $result = $this->client->apiGet('workspaces');
        assert(is_array($result));
        return $result;
    }

    /**
     * @param int $id
     * @return array{id: int, connection: array}
     */
    public function getWorkspace($id)
    {
        /** @var array{id: int, connection: array} $result */
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

    /**
     * @param int $id
     * @return array
     */
    public function resetWorkspacePassword($id)
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
}
