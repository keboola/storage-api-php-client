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
use Keboola\StorageApi\Options\Components\ListConfigurationWorkspacesOptions;

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
        return $this->client->apiPost("components/{$options->getComponentId()}/configs", array(
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

        if ($options->getConfiguration() !== null) {
            if ($options->getConfiguration() === []) {
                $data['configuration'] = '{}';
            } else {
                $data['configuration'] = json_encode($options->getConfiguration());
            }
        }

        if (!is_null($options->getState())) {
            $data['state'] = json_encode($options->getState());
        }

        if ($options->getChangeDescription()) {
            $data['changeDescription'] = $options->getChangeDescription();
        }

        if (count($options->getRowsSortOrder()) > 0) {
            $data['rowsSortOrder'] = $options->getRowsSortOrder();
        }

        return $this->client->apiPut(
            "components/{$options->getComponentId()}/configs/{$options->getConfigurationId()}",
            $data
        );
    }

    public function publishConfiguration($componentId, $configurationId, $description)
    {
        return $this->client->apiPost(
            "components/{$componentId}/configs/{$configurationId}/publish",
            [
                'description' => $description,
            ]
        );
    }

    public function getConfiguration($componentId, $configurationId)
    {
        return $this->client->apiGet("components/{$componentId}/configs/{$configurationId}");
    }

    public function deleteConfiguration($componentId, $configurationId)
    {
        return $this->client->apiDelete("components/{$componentId}/configs/{$configurationId}");
    }

    public function listComponents(ListComponentsOptions $options = null)
    {
        if (!$options) {
            $options = new ListComponentsOptions();
        }
        return $this->client->apiGet("components?" . http_build_query($options->toParamsArray()));
    }

    public function listComponentConfigurations(ListComponentConfigurationsOptions $options)
    {
        return $this->client->apiGet("components/{$options->getComponentId()}/configs?" . http_build_query($options->toParamsArray()));
    }

    public function restoreComponentConfiguration($componentId, $configurationId)
    {
        return $this->client->apiPost("components/{$componentId}/configs/{$configurationId}/restore");
    }

    public function listConfigurationVersions(ListConfigurationVersionsOptions $options)
    {
        return $this->client->apiGet("components/{$options->getComponentId()}/configs/"
            . "{$options->getConfigurationId()}/versions?" . http_build_query($options->toParamsArray()));
    }

    public function getConfigurationVersion($componentId, $configurationId, $version)
    {
        return $this->client->apiGet("components/{$componentId}/configs/{$configurationId}/versions/{$version}");
    }

    public function rollbackConfiguration($componentId, $configurationId, $version, $changeDescription = null)
    {
        return $this->client->apiPost(
            "components/{$componentId}/configs/{$configurationId}/versions/{$version}/rollback",
            array('changeDescription' => $changeDescription)
        );
    }

    public function createConfigurationFromVersion($componentId, $configurationId, $version, $name, $description = null, $changeDescription = null)
    {
        return $this->client->apiPost(
            "components/{$componentId}/configs/{$configurationId}/versions/{$version}/create",
            array('name' => $name, 'description' => $description, 'changeDescription' => $changeDescription)
        );
    }

    public function getConfigurationRow($componentId, $configurationId, $rowId)
    {
        return $this->client->apiGet(sprintf(
            "components/%s/configs/%s/rows/%s",
            $componentId,
            $configurationId,
            $rowId
        ));
    }

    public function listConfigurationRows(ListConfigurationRowsOptions $options = null)
    {
        if (!$options) {
            $options = new ListConfigurationRowsOptions();
        }
        return $this->client->apiGet("components/{$options->getComponentId()}/configs/"
            . "{$options->getConfigurationId()}/rows");
    }

    public function listConfigurationWorkspaces(ListConfigurationWorkspacesOptions $options = null)
    {
        if (!$options) {
            $options = new ListConfigurationWorkspacesOptions();
        }
        return $this->client->apiGet("components/{$options->getComponentId()}/configs/"
            . "{$options->getConfigurationId()}/workspaces");
    }

    public function addConfigurationRow(ConfigurationRow $options)
    {
        return $this->client->apiPost(
            sprintf(
                "components/%s/configs/%s/rows",
                $options->getComponentConfiguration()->getComponentId(),
                $options->getComponentConfiguration()->getConfigurationId()
            ),
            array(
                'rowId' => $options->getRowId(),
                'configuration' => $options->getConfiguration() ? json_encode($options->getConfiguration()) : null,
                'state' => $options->getState() ? json_encode($options->getState()) : null,
                'changeDescription' => $options->getChangeDescription(),
                'name' => $options->getName(),
                'description' => $options->getDescription(),
                'isDisabled' => $options->getIsDisabled()
            )
        );
    }

    public function deleteConfigurationRow($componentId, $configurationId, $rowId, $changeDescription = null)
    {
        return $this->client->apiDeleteParams(
            "components/{$componentId}/configs/{$configurationId}/rows/{$rowId}",
            array(
                'changeDescription' => $changeDescription
            )
        );
    }

    public function updateConfigurationRow(ConfigurationRow $options)
    {
        $data = array();
        if ($options->getName() !== null) {
            $data['name'] = $options->getName();
        }

        if ($options->getDescription() !== null) {
            $data['description'] = $options->getDescription();
        }

        if ($options->getConfiguration() !== null) {
            if ($options->getConfiguration() === []) {
                $data['configuration'] = '{}';
            } else {
                $data['configuration'] = json_encode($options->getConfiguration());
            }
        }

        if ($options->getState() !== null) {
            if ($options->getState() === []) {
                $data['state'] = '{}';
            } else {
                $data['state'] = json_encode($options->getState());
            }
        }


        if ($options->getIsDisabled() !== null) {
            $data['isDisabled'] = $options->getIsDisabled();
        }

        if ($options->getChangeDescription()) {
            $data['changeDescription'] = $options->getChangeDescription();
        }

        return $this->client->apiPut(
            sprintf(
                "components/%s/configs/%s/rows/%s",
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
                "components/%s/configs/%s/rows/%s/versions?%s",
                $options->getComponentId(),
                $options->getConfigurationId(),
                $options->getRowId(),
                http_build_query($options->toParamsArray())
            )
        );
    }

    public function getConfigurationRowVersion($componentId, $configurationId, $rowId, $version)
    {
        return $this->client->apiGet("components/{$componentId}/configs/{$configurationId}/rows/{$rowId}/versions/{$version}");
    }

    public function rollbackConfigurationRow($componentId, $configurationId, $rowId, $version, $changeDescription = null)
    {
        return $this->client->apiPost(
            "components/{$componentId}/configs/{$configurationId}/rows/{$rowId}/versions/{$version}/rollback",
            array("changeDescription" => $changeDescription)
        );
    }

    public function createConfigurationRowFromVersion($componentId, $configurationId, $rowId, $version, $targetConfigurationId = null, $changeDescription = null)
    {
        return $this->client->apiPost(
            "components/{$componentId}/configs/{$configurationId}/rows/{$rowId}/versions/{$version}/create",
            array('targetConfigId' => $targetConfigurationId, 'changeDescription' => $changeDescription)
        );
    }

    public function createConfigurationWorkspace($componentId, $configurationId, array $options = [])
    {
        return $this->client->apiPost(
            "components/{$componentId}/configs/{$configurationId}/workspaces",
            $options
        );
    }
}
