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
use Keboola\StorageApi\Options\Components\ListConfigurationsOptions;

class Components {

	/**
	 * @var Client
	 */
	private $client;

	public function __construct(Client $client)
	{
		$this->client = $client;
	}

	public function addConfiguration(Configuration $options)
	{
		return $this->client->apiPost("storage/components/{$options->getComponentId()}/configs", array(
			'name' => $options->getName(),
			'description' => $options->getDescription(),
			'configurationId' => $options->getConfigurationId(),
			'configuration' => $options->getConfiguration() ? json_encode($options->getConfiguration()) : null,
		));
	}

	public function updateConfiguration(Configuration $options)
	{
		$data = array();
		if ($options->getName() !== null) {
			$data['name'] = $options->getName();
		}

		if ($options->getDescription() !== null) {
			$data['description'] = $options->getDescription();
		}

		if ($options->getConfiguration()) {
			$data['configuration'] = json_encode($options->getConfiguration());
		}

		return $this->client->apiPut(
			"storage/components/{$options->getComponentId()}/configs/{$options->getConfigurationId()}",
			$data
		);
	}

	public function getConfiguration($componentId, $configurationId)
	{
		return $this->client->apiGet("storage/components/{$componentId}/configs/{$configurationId}");
	}

	public function deleteConfiguration($componentId, $configurationId)
	{
		return $this->client->apiDelete("storage/components/{$componentId}/configs/{$configurationId}");
	}

	public function listComponents(ListConfigurationsOptions $options = null)
	{
		if (!$options) {
			$options = new ListConfigurationsOptions();
		}
		return $this->client->apiGet("storage/components?" . http_build_query($options->toParamsArray()));
	}

}