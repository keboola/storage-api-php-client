<?php
/**
 * Created by JetBrains PhpStorm.
 * User: martinhalamicek
 * Date: 16/09/14
 * Time: 01:48
 * To change this template use File | Settings | File Templates.
 */

namespace Keboola\StorageApi;


use Keboola\StorageApi\Options\Components\Configuration;
use Keboola\StorageApi\Options\Components\ConfigurationRow;
use Keboola\StorageApi\Options\Components\ListConfigurationRowsOptions;
use Keboola\StorageApi\Options\Components\ListConfigurationRowVersionsOptions;
use Keboola\StorageApi\Options\Components\ListConfigurationsOptions;
use Keboola\StorageApi\Options\Components\ListConfigurationVersionsOptions;

class Workspaces {

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


	public function deleteWorkspace($id)
	{
		$this->client->apiDelete("storage/workspaces/{$id}");
	}

}
