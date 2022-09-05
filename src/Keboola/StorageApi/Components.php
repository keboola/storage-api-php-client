<?php
namespace Keboola\StorageApi;

use Keboola\StorageApi\Options\Components\Configuration;
use Keboola\StorageApi\Options\Components\ConfigurationMetadata;
use Keboola\StorageApi\Options\Components\ConfigurationRow;
use Keboola\StorageApi\Options\Components\ConfigurationRowState;
use Keboola\StorageApi\Options\Components\ConfigurationState;
use Keboola\StorageApi\Options\Components\ListComponentConfigurationsOptions;
use Keboola\StorageApi\Options\Components\ListConfigurationMetadataOptions;
use Keboola\StorageApi\Options\Components\ListConfigurationRowsOptions;
use Keboola\StorageApi\Options\Components\ListConfigurationRowVersionsOptions;
use Keboola\StorageApi\Options\Components\ListComponentsOptions;
use Keboola\StorageApi\Options\Components\ListConfigurationVersionsOptions;
use Keboola\StorageApi\Options\Components\ListConfigurationWorkspacesOptions;

class Components
{
    private string $branchPrefix = '';

    /**
     * @var Client
     */
    private $client;

    public function __construct(Client $client)
    {
        $this->client = $client;
        if (!$client instanceof BranchAwareClient) {
            $this->branchPrefix = 'branch/default/';
        }
    }

    public function addConfiguration(Configuration $options)
    {
        return $this->client->apiPost($this->branchPrefix . "components/{$options->getComponentId()}/configs", [
            'name' => $options->getName(),
            'description' => $options->getDescription(),
            'configurationId' => $options->getConfigurationId(),
            'configuration' => $options->getConfiguration() ? json_encode($options->getConfiguration()) : null,
            'state' => $options->getState() ? json_encode($options->getState()) : null,
            'changeDescription' => $options->getChangeDescription(),
            'isDisabled' => $options->getIsDisabled(),
        ]);
    }

    public function updateConfiguration(Configuration $options)
    {
        $data = [];
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

        if ($options->getIsDisabled() !== null) {
            $data['isDisabled'] = $options->getIsDisabled();
        }

        if (count($options->getRowsSortOrder()) > 0) {
            $data['rowsSortOrder'] = $options->getRowsSortOrder();
        }

        return $this->client->apiPut(
            $this->branchPrefix . "components/{$options->getComponentId()}/configs/{$options->getConfigurationId()}",
            $data
        );
    }

    public function updateConfigurationState(ConfigurationState $options)
    {
        $data = [];

        if ($options->getState() !== null) {
            if ($options->getState() === []) {
                $data['state'] = '{}';
            } else {
                $data['state'] = json_encode($options->getState());
            }
        }

        return $this->client->apiPut(
            $this->branchPrefix . "components/{$options->getComponentId()}/configs/{$options->getConfigurationId()}/state",
            $data
        );
    }

    public function getConfiguration($componentId, $configurationId)
    {
        return $this->client->apiGet($this->branchPrefix . "components/{$componentId}/configs/{$configurationId}");
    }

    public function deleteConfiguration($componentId, $configurationId)
    {
        return $this->client->apiDelete($this->branchPrefix . "components/{$componentId}/configs/{$configurationId}");
    }

    public function resetToDefault($componentId, $configurationId)
    {
        return $this->client->apiPost($this->branchPrefix . "components/{$componentId}/configs/{$configurationId}/reset-to-default");
    }

    public function listComponents(ListComponentsOptions $options = null)
    {
        if (!$options) {
            $options = new ListComponentsOptions();
        }
        return $this->client->apiGet($this->branchPrefix . 'components?' . http_build_query($options->toParamsArray()));
    }

    public function getComponent($componentId)
    {
        return $this->client->apiGet($this->branchPrefix . "components/{$componentId}");
    }

    public function listComponentConfigurations(ListComponentConfigurationsOptions $options)
    {
        return $this->client->apiGet($this->branchPrefix . "components/{$options->getComponentId()}/configs?" . http_build_query($options->toParamsArray()));
    }

    public function restoreComponentConfiguration($componentId, $configurationId)
    {
        return $this->client->apiPost($this->branchPrefix . "components/{$componentId}/configs/{$configurationId}/restore");
    }

    public function listConfigurationVersions(ListConfigurationVersionsOptions $options)
    {
        return $this->client->apiGet($this->branchPrefix . "components/{$options->getComponentId()}/configs/"
            . "{$options->getConfigurationId()}/versions?" . http_build_query($options->toParamsArray()));
    }

    public function getConfigurationVersion($componentId, $configurationId, $version)
    {
        return $this->client->apiGet($this->branchPrefix . "components/{$componentId}/configs/{$configurationId}/versions/{$version}");
    }

    public function rollbackConfiguration($componentId, $configurationId, $version, $changeDescription = null)
    {
        return $this->client->apiPost(
            $this->branchPrefix . "components/{$componentId}/configs/{$configurationId}/versions/{$version}/rollback",
            ['changeDescription' => $changeDescription]
        );
    }

    public function createConfigurationFromVersion($componentId, $configurationId, $version, $name, $description = null, $changeDescription = null)
    {
        return $this->client->apiPost(
            $this->branchPrefix . "components/{$componentId}/configs/{$configurationId}/versions/{$version}/create",
            ['name' => $name, 'description' => $description, 'changeDescription' => $changeDescription]
        );
    }

    public function getConfigurationRow($componentId, $configurationId, $rowId)
    {
        return $this->client->apiGet(sprintf(
            $this->branchPrefix . 'components/%s/configs/%s/rows/%s',
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
        return $this->client->apiGet($this->branchPrefix . "components/{$options->getComponentId()}/configs/"
            . "{$options->getConfigurationId()}/rows");
    }

    public function listConfigurationWorkspaces(ListConfigurationWorkspacesOptions $options = null)
    {
        if (!$options) {
            $options = new ListConfigurationWorkspacesOptions();
        }
        return $this->client->apiGet($this->branchPrefix . "components/{$options->getComponentId()}/configs/"
            . "{$options->getConfigurationId()}/workspaces");
    }

    public function addConfigurationRow(ConfigurationRow $options)
    {
        return $this->client->apiPostJson(
            sprintf(
                $this->branchPrefix . 'components/%s/configs/%s/rows',
                $options->getComponentConfiguration()->getComponentId(),
                $options->getComponentConfiguration()->getConfigurationId()
            ),
            [
                'rowId' => $options->getRowId(),
                'configuration' => $options->getConfiguration() ? json_encode($options->getConfiguration()) : null,
                'state' => $options->getState() ? json_encode($options->getState()) : null,
                'changeDescription' => $options->getChangeDescription(),
                'name' => $options->getName(),
                'description' => $options->getDescription(),
                'isDisabled' => $options->getIsDisabled(),
            ]
        );
    }

    public function deleteConfigurationRow($componentId, $configurationId, $rowId, $changeDescription = null)
    {
        return $this->client->apiDeleteParamsJson(
            $this->branchPrefix . "components/{$componentId}/configs/{$configurationId}/rows/{$rowId}",
            [
                'changeDescription' => $changeDescription,
            ]
        );
    }

    public function updateConfigurationRow(ConfigurationRow $options)
    {
        $data = [];
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
                $this->branchPrefix . 'components/%s/configs/%s/rows/%s',
                $options->getComponentConfiguration()->getComponentId(),
                $options->getComponentConfiguration()->getConfigurationId(),
                $options->getRowId()
            ),
            $data
        );
    }

    public function updateConfigurationRowState(ConfigurationRowState $options)
    {
        $data = [];

        if ($options->getState() !== null) {
            if ($options->getState() === []) {
                $data['state'] = '{}';
            } else {
                $data['state'] = json_encode($options->getState());
            }
        }

        return $this->client->apiPutJson(
            sprintf(
                $this->branchPrefix . 'components/%s/configs/%s/rows/%s/state',
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
                $this->branchPrefix . 'components/%s/configs/%s/rows/%s/versions?%s',
                $options->getComponentId(),
                $options->getConfigurationId(),
                $options->getRowId(),
                http_build_query($options->toParamsArray())
            )
        );
    }

    public function getConfigurationRowVersion($componentId, $configurationId, $rowId, $version)
    {
        return $this->client->apiGet($this->branchPrefix . "components/{$componentId}/configs/{$configurationId}/rows/{$rowId}/versions/{$version}");
    }

    public function rollbackConfigurationRow($componentId, $configurationId, $rowId, $version, $changeDescription = null)
    {
        return $this->client->apiPost(
            $this->branchPrefix . "components/{$componentId}/configs/{$configurationId}/rows/{$rowId}/versions/{$version}/rollback",
            ['changeDescription' => $changeDescription]
        );
    }

    public function createConfigurationRowFromVersion($componentId, $configurationId, $rowId, $version, $targetConfigurationId = null, $changeDescription = null)
    {
        return $this->client->apiPost(
            $this->branchPrefix . "components/{$componentId}/configs/{$configurationId}/rows/{$rowId}/versions/{$version}/create",
            ['targetConfigId' => $targetConfigurationId, 'changeDescription' => $changeDescription]
        );
    }

    public function createConfigurationWorkspace($componentId, $configurationId, array $options = [])
    {
        return $this->client->apiPost(
            $this->branchPrefix . "components/{$componentId}/configs/{$configurationId}/workspaces",
            $options,
            true,
            [Client::REQUEST_OPTION_EXTENDED_TIMEOUT => true]
        );
    }

    public function addConfigurationMetadata(ConfigurationMetadata $options)
    {
        return $this->client->apiPost(
            sprintf(
                $this->branchPrefix . 'components/%s/configs/%s/metadata',
                $options->getComponentConfiguration()->getComponentId(),
                $options->getComponentConfiguration()->getConfigurationId()
            ),
            [
                'metadata' => $options->getMetadata(),
            ]
        );
    }

    public function listConfigurationMetadata(ListConfigurationMetadataOptions $options)
    {
        return $this->client->apiGet(sprintf(
            $this->branchPrefix . 'components/%s/configs/%s/metadata',
            $options->getComponentId(),
            $options->getConfigurationId()
        ));
    }

    public function deleteConfigurationMetadata($componentId, $configurationId, $metadataId)
    {
        return $this->client->apiDelete(sprintf(
            $this->branchPrefix . 'components/%s/configs/%s/metadata/%s',
            $componentId,
            $configurationId,
            $metadataId
        ));
    }
}
