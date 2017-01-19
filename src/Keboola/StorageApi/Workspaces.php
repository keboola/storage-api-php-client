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
     *  - name (optional)
     *  - backend (optional)
     */
    public function createWorkspace(array $options = [])
    {
        return $this->client->apiPost("storage/workspaces", $options);
    }

    public function listWorkspaces()
    {
        return $this->client->apiGet("storage/workspaces");
    }

    public function getWorkspace($id)
    {
        return $this->client->apiGet("storage/workspaces/{$id}");
    }


    public function deleteWorkspace($id, array $options = [])
    {
        $this->client->apiDelete("storage/workspaces/{$id}?" . http_build_query($options));
    }

    /**
     * @param $id
     * @param array $options -- required input[mappings], optional preserve
     * @return mixed|string
     */
    public function loadWorkspaceData($id, array $options = [])
    {
        return $this->client->apiPost("storage/workspaces/{$id}/load", $options);
    }
}
