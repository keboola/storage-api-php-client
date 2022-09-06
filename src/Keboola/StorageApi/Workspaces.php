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
     * @param array $options
     *  - backend (optional)
     */
    public function createWorkspace(array $options = [], bool $async = false)
    {
        $url = 'workspaces';
        $requestOptions = [Client::REQUEST_OPTION_EXTENDED_TIMEOUT => true];
        if ($async) {
            $url .= '?' . http_build_query(['async' => $async]);
            $requestOptions = [];
        }

        return $this->client->apiPostJson($url, $options, true, $requestOptions);
    }

    public function listWorkspaces()
    {
        return $this->client->apiGet('workspaces');
    }

    public function getWorkspace($id)
    {
        return $this->client->apiGet("workspaces/{$id}");
    }

    public function deleteWorkspace($id, array $options = [], bool $async = false): void
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
     * @param $id
     * @param array $options -- required input[mappings], optional preserve
     * @return mixed|string
     */
    public function loadWorkspaceData($id, array $options = [])
    {
        return $this->client->apiPostJson("workspaces/{$id}/load", $options);
    }

    /**
     * @param $id
     * @param array $options -- required input[mappings], optional preserve
     * @return mixed|string
     */
    public function cloneIntoWorkspace($id, array $options = [])
    {
        return $this->client->apiPostJson("workspaces/{$id}/load-clone", $options);
    }

    public function resetWorkspacePassword($id)
    {
        return $this->client->apiPostJson("workspaces/{$id}/password");
    }

    /**
     * @param array $options -- required input[mappings], optional preserve
     */
    public function queueWorkspaceLoadData(int $id, array $options = []): int
    {
        /** @var array{id: int} $job */
        $job = $this->client->apiPost("workspaces/{$id}/load", $options, false);
        return (int) $job['id'];
    }

    /**
     * @param array $options -- required input[mappings], optional preserve
     */
    public function queueWorkspaceCloneInto(int $id, array $options = []): int
    {
        /** @var array{id: int} $job */
        $job = $this->client->apiPost("workspaces/{$id}/load-clone", $options, false);
        return (int) $job['id'];
    }
}
