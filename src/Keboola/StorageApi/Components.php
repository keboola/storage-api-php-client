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
use Keboola\StorageApi\Options\Components\ListComponentConfigurationsOptions;
use Keboola\StorageApi\Options\Components\ListConfigurationRowsOptions;
use Keboola\StorageApi\Options\Components\ListConfigurationRowVersionsOptions;
use Keboola\StorageApi\Options\Components\ListComponentsOptions;
use Keboola\StorageApi\Options\Components\ListConfigurationVersionsOptions;

class Components
{

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
            'state' => $options->getState() ? json_encode($options->getState()) : null,
            'changeDescription' => $options->getChangeDescription()
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

        if ($options->getState()) {
            $data['state'] = json_encode($options->getState());
        }

        if ($options->getChangeDescription()) {
            $data['changeDescription'] = $options->getChangeDescription();
        }

        return $this->client->apiPut(
            "storage/components/{$options->getComponentId()}/configs/{$options->getConfigurationId()}",
            $data
        );
    }

    public function publishConfiguration($componentId, $configurationId, $description)
    {
        return $this->client->apiPost(
            "storage/components/{$componentId}/configs/{$configurationId}/publish",
            [
                'description' => $description,
            ]
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

    public function listComponents(ListComponentsOptions $options = null)
    {
        if (!$options) {
            $options = new ListComponentsOptions();
        }
        return $this->client->apiGet("storage/components?" . http_build_query($options->toParamsArray()));
    }

    public function listComponentConfigurations(ListComponentConfigurationsOptions $options)
    {
        return $this->client->apiGet("storage/components/{$options->getComponentId()}/configs?" . http_build_query($options->toParamsArray()));
    }

    public function restoreComponentConfiguration($componentId, $configurationId)
    {
        return $this->client->apiPost("storage/components/{$componentId}/configs/{$configurationId}/restore");
    }

    public function listConfigurationVersions(ListConfigurationVersionsOptions $options)
    {
        return $this->client->apiGet("storage/components/{$options->getComponentId()}/configs/"
            . "{$options->getConfigurationId()}/versions?" . http_build_query($options->toParamsArray()));
    }

    public function getConfigurationVersion($componentId, $configurationId, $version)
    {
        return $this->client->apiGet("storage/components/{$componentId}/configs/{$configurationId}/versions/{$version}");
    }

    public function rollbackConfiguration($componentId, $configurationId, $version, $changeDescription = null)
    {
        return $this->client->apiPost(
            "storage/components/{$componentId}/configs/{$configurationId}/versions/{$version}/rollback",
            array('changeDescription' => $changeDescription)
        );
    }

    public function createConfigurationFromVersion($componentId, $configurationId, $version, $name, $description = null, $changeDescription = null)
    {
        return $this->client->apiPost(
            "storage/components/{$componentId}/configs/{$configurationId}/versions/{$version}/create",
            array('name' => $name, 'description' => $description, 'changeDescription' => $changeDescription)
        );
    }

    public function listConfigurationRows(ListConfigurationRowsOptions $options = null)
    {
        if (!$options) {
            $options = new ListConfigurationRowsOptions();
        }
        return $this->client->apiGet("storage/components/{$options->getComponentId()}/configs/"
            . "{$options->getConfigurationId()}/rows");
    }

    public function addConfigurationRow(ConfigurationRow $options)
    {
        return $this->client->apiPost(
            sprintf(
                "storage/components/%s/configs/%s/rows",
                $options->getComponentConfiguration()->getComponentId(),
                $options->getComponentConfiguration()->getConfigurationId()
            ),
            array(
                'rowId' => $options->getRowId(),
                'configuration' => $options->getConfiguration() ? json_encode($options->getConfiguration()) : null,
                'changeDescription' => $options->getChangeDescription(),
            )
        );
    }

    public function deleteConfigurationRow($componentId, $configurationId, $rowId, $changeDescription = null)
    {
        return $this->client->apiDeleteParams(
            "storage/components/{$componentId}/configs/{$configurationId}/rows/{$rowId}",
            array(
                'changeDescription' => $changeDescription
            )
        );
    }

    public function updateConfigurationRow(ConfigurationRow $options)
    {
        $data = array();

        $data['configuration'] = $options->getConfiguration() ? json_encode($options->getConfiguration()) : null;

        if ($options->getChangeDescription()) {
            $data['changeDescription'] = $options->getChangeDescription();
        }

        return $this->client->apiPut(
            sprintf(
                "storage/components/%s/configs/%s/rows/%s",
                $options->getComponentConfiguration()->getComponentId(),
                $options->getComponentConfiguration()->getConfigurationId(),
                $options->getRowId()
            ),
            $data
        );
    }

    public function listConfigurationRowVersions(ListConfigurationRowVersionsOptions $options)
    {
        return $this->client->apiGet(
            sprintf(
                "storage/components/%s/configs/%s/rows/%s/versions?%s",
                $options->getComponentId(),
                $options->getConfigurationId(),
                $options->getRowId(),
                http_build_query($options->toParamsArray())
            )
        );
    }

    public function getConfigurationRowVersion($componentId, $configurationId, $rowId, $version)
    {
        return $this->client->apiGet("storage/components/{$componentId}/configs/{$configurationId}/rows/{$rowId}/versions/{$version}");
    }

    public function rollbackConfigurationRow($componentId, $configurationId, $rowId, $version, $changeDescription = null)
    {
        return $this->client->apiPost(
            "storage/components/{$componentId}/configs/{$configurationId}/rows/{$rowId}/versions/{$version}/rollback",
            array("changeDescription" => $changeDescription)
        );
    }

    public function createConfigurationRowFromVersion($componentId, $configurationId, $rowId, $version, $targetConfigurationId = null, $changeDescription = null)
    {
        return $this->client->apiPost(
            "storage/components/{$componentId}/configs/{$configurationId}/rows/{$rowId}/versions/{$version}/create",
            array('targetConfigId' => $targetConfigurationId, 'changeDescription' => $changeDescription)
        );
    }
}
