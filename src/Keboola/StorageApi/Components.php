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

/**
 * @phpstan-type Component array{id: string, type: string, name: string, description: string, longDescription: string, version: int, complexity: string, categories: list<string>, hasUI: bool, hasRun: bool, ico32: string, ico64: string, ico128: string, data: array, flags: list<string>, configurationSchema: array, configurationRowSchema: array, emptyConfiguration: array, emptyConfigurationRow: array, createConfigurationRowSchema: array, uiOptions: array, configurationDescription: string, features: list<string>, expiredOn: string, dataTypesConfiguration: array, processorConfiguration: array, uri: string, documentationUrl: string}
 * @phpstan-type ComponentListItem array{id: string, type: string, name: string, description: string, longDescription: string, version: int, complexity: string, categories: list<string>, hasUI: bool, hasRun: bool, ico32: string, ico64: string, ico128: string, data: array, flags: list<string>, configurationSchema: array, configurationRowSchema: array, emptyConfiguration: array, emptyConfigurationRow: array, createConfigurationRowSchema: array, uiOptions: array, configurationDescription: string, features: list<string>, expiredOn: string, dataTypesConfiguration: array, configurations: array, processorConfiguration: array, uri: string, documentationUrl: string}
 * @phpstan-type ComponentConfiguration array{id: string, name: string, description: string, created: string, creatorToken: array{id: int, description: string}, version: int, changeDescription: string|null, isDisabled: bool, isDeleted: bool, configuration: array<string, mixed>, state: array<string, mixed>, rowsSortOrder: list<string>, rows: list<array<string, mixed>>, currentVersion: array{created: string, creatorToken: array{id: int, description: string}, changeDescription: string|null, versionIdentifier: string|null}}
 */
class Components
{
    public const HEADER_IF_MATCH = 'If-Match';

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
        return $this->client->apiPostJson($this->branchPrefix . "components/{$options->getComponentId()}/configs", [
            'name' => $options->getName(),
            'description' => $options->getDescription(),
            'configurationId' => $options->getConfigurationId(),
            'configuration' => $options->getConfiguration() ?: null,
            'state' => $options->getState() ?: null,
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
                $data['configuration'] = (object) [];
            } else {
                $data['configuration'] = $options->getConfiguration();
            }
        }

        if ($options->getState() !== null) {
            $data['state'] = $options->getState();
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

        $requestOptions = [];
        if ($options->getExpectedVersion() !== null) {
            $requestOptions[Client::REQUEST_OPTION_HEADERS] = [
                self::HEADER_IF_MATCH => sprintf('"%d"', $options->getExpectedVersion()),
            ];
        }

        return $this->client->apiPutJson(
            $this->branchPrefix . "components/{$options->getComponentId()}/configs/{$options->getConfigurationId()}",
            $data,
            true,
            $requestOptions,
        );
    }

    public function updateConfigurationState(ConfigurationState $options)
    {
        $data = [];

        if ($options->getState() !== null) {
            if ($options->getState() === []) {
                $data['state'] = (object) [];
            } else {
                $data['state'] = $options->getState();
            }
        }

        return $this->client->apiPutJson(
            $this->branchPrefix . "components/{$options->getComponentId()}/configs/{$options->getConfigurationId()}/state",
            $data,
        );
    }

    public function getConfiguration($componentId, $configurationId)
    {
        return $this->client->apiGet($this->branchPrefix . "components/{$componentId}/configs/{$configurationId}");
    }

    /**
     * Moves the configuration to the trash; a second call on an already trashed configuration
     * purges it permanently. $expectedVersion guards both transitions: the call is rejected with
     * 412 unless the configuration is still at that version. Take it from the `version` field of
     * a configuration detail; null leaves the delete unconditional.
     *
     * @param string $componentId
     * @param string $configurationId
     * @param int|null $expectedVersion
     * @return mixed|string
     */
    public function deleteConfiguration($componentId, $configurationId, ?int $expectedVersion = null)
    {
        return $this->client->apiDelete(
            $this->branchPrefix . "components/{$componentId}/configs/{$configurationId}",
            true,
            $this->expectedVersionRequestOptions($expectedVersion),
        );
    }

    /**
     * Permanently removes a trashed configuration, including its versions, rows and metadata.
     * Unlike a repeated deleteConfiguration() call, this fails with 400 when the configuration is
     * not in the trash, so a stale caller cannot destroy a live configuration. Default branch
     * only; requires the `canPurgeTrash` token permission. $expectedVersion is required — the purge
     * is applied only if the configuration is still at that version (412 otherwise); take it from
     * the `version` field of the trash listing.
     *
     * @param string $componentId
     * @param string $configurationId
     * @return mixed|string
     */
    public function purgeConfiguration($componentId, $configurationId, int $expectedVersion)
    {
        return $this->client->apiPostJson(
            $this->branchPrefix . "components/{$componentId}/configs/{$configurationId}/purge",
            [],
            true,
            $this->expectedVersionRequestOptions($expectedVersion),
        );
    }

    /**
     * @return array<string, array<string, string>>
     */
    private function expectedVersionRequestOptions(?int $expectedVersion): array
    {
        if ($expectedVersion === null) {
            return [];
        }

        return [
            Client::REQUEST_OPTION_HEADERS => [
                self::HEADER_IF_MATCH => sprintf('"%d"', $expectedVersion),
            ],
        ];
    }

    public function resetToDefault($componentId, $configurationId)
    {
        return $this->client->apiPostJson($this->branchPrefix . "components/{$componentId}/configs/{$configurationId}/reset-to-default");
    }

    /**
     * @return array{
     *     base: array{version: int, isDeleted: bool, diff: array<string, mixed>}|null,
     *     ours: array{version: int, isDeleted: bool, diff: array<string, mixed>}|null,
     *     theirs: array{version: int, isDeleted: bool, diff: array<string, mixed>}|null,
     * }
     */
    public function getConfigurationDiff(string $componentId, string $configurationId): array
    {
        return $this->client->apiGet($this->branchPrefix . "components/{$componentId}/configs/{$configurationId}/diff");
    }

    /**
     * Rebases the dev branch configuration onto the given default branch version, keeping the resolved head content.
     * To resolve the conflict by deleting the configuration instead, use rebaseConfigurationToDeleted().
     *
     * $rows is the complete resolved row set (the diff's `diff.rows` posted back) and is required for a keep
     * rebase: an empty array deletes all rows, the array order becomes the row sort order. A row without an id is
     * created with a generated id.
     *
     * @param list<array{id?: string, name?: string, description?: string, isDisabled?: bool, configuration?: array<string, mixed>|object}> $rows
     * @param array<string, mixed>|null $configuration
     * @return ComponentConfiguration
     */
    public function rebaseConfiguration(
        string $componentId,
        string $configurationId,
        int $version,
        string $name,
        array $rows,
        ?string $description = null,
        ?array $configuration = null,
        ?string $changeDescription = null,
        bool $isDisabled = false,
    ): array {
        return $this->client->apiPostJson(
            $this->branchPrefix . "components/{$componentId}/configs/{$configurationId}/rebase",
            [
                'version' => $version,
                'diff' => [
                    'name' => $name,
                    'rows' => array_values($rows),
                    'description' => $description,
                    'configuration' => $configuration === null ? null : ($configuration ?: (object) []),
                    'changeDescription' => $changeDescription,
                    'isDisabled' => $isDisabled,
                ],
            ],
        );
    }

    /**
     * Rebases the dev branch configuration onto the given default branch version, resolving the conflict by deleting
     * the configuration: the request carries an empty "diff" (no resolved content), so the new head version is a
     * tombstone (isDeleted = true).
     *
     * @return ComponentConfiguration
     */
    public function rebaseConfigurationToDeleted(
        string $componentId,
        string $configurationId,
        int $version,
    ): array {
        return $this->client->apiPostJson(
            $this->branchPrefix . "components/{$componentId}/configs/{$configurationId}/rebase",
            [
                'version' => $version,
                'diff' => (object) [],
            ],
        );
    }

    /** @return ComponentListItem[] */
    public function listComponents(?ListComponentsOptions $options = null): array
    {
        if (!$options) {
            $options = new ListComponentsOptions();
        }
        /** @var ComponentListItem[] $result */
        $result = $this->client->apiGet($this->branchPrefix . 'components?' . http_build_query($options->toParamsArray()));
        return $result;
    }

    /** @return Component */
    public function getComponent($componentId): array
    {
        /** @var Component $result */
        $result = $this->client->apiGet($this->branchPrefix . "components/{$componentId}");
        return $result;
    }

    /** @return Component */
    public function getPublicComponentDetail(string $componentId): array
    {
        /** @var Component $result */
        $result = $this->client->apiGet('components/'.$componentId);
        return $result;
    }

    public function listComponentConfigurations(ListComponentConfigurationsOptions $options)
    {
        return $this->client->apiGet($this->branchPrefix . "components/{$options->getComponentId()}/configs?" . http_build_query($options->toParamsArray()));
    }

    public function restoreComponentConfiguration($componentId, $configurationId)
    {
        return $this->client->apiPostJson($this->branchPrefix . "components/{$componentId}/configs/{$configurationId}/restore");
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
        return $this->client->apiPostJson(
            $this->branchPrefix . "components/{$componentId}/configs/{$configurationId}/versions/{$version}/rollback",
            ['changeDescription' => $changeDescription],
        );
    }

    public function createConfigurationFromVersion($componentId, $configurationId, $version, $name, $description = null, $changeDescription = null)
    {
        return $this->client->apiPostJson(
            $this->branchPrefix . "components/{$componentId}/configs/{$configurationId}/versions/{$version}/create",
            ['name' => $name, 'description' => $description, 'changeDescription' => $changeDescription],
        );
    }

    public function getConfigurationRow($componentId, $configurationId, $rowId)
    {
        return $this->client->apiGet(sprintf(
            $this->branchPrefix . 'components/%s/configs/%s/rows/%s',
            $componentId,
            $configurationId,
            $rowId,
        ));
    }

    public function listConfigurationRows(?ListConfigurationRowsOptions $options = null)
    {
        if (!$options) {
            $options = new ListConfigurationRowsOptions();
        }
        return $this->client->apiGet($this->branchPrefix . "components/{$options->getComponentId()}/configs/"
            . "{$options->getConfigurationId()}/rows");
    }

    public function listConfigurationWorkspaces(?ListConfigurationWorkspacesOptions $options = null)
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
                $options->getComponentConfiguration()->getConfigurationId(),
            ),
            [
                'rowId' => $options->getRowId(),
                'configuration' => $options->getConfiguration() ?: null,
                'state' => $options->getState() ?: null,
                'changeDescription' => $options->getChangeDescription(),
                'name' => $options->getName(),
                'description' => $options->getDescription(),
                'isDisabled' => $options->getIsDisabled(),
            ],
        );
    }

    public function deleteConfigurationRow($componentId, $configurationId, $rowId, $changeDescription = null)
    {
        return $this->client->apiDeleteParamsJson(
            $this->branchPrefix . "components/{$componentId}/configs/{$configurationId}/rows/{$rowId}",
            [
                'changeDescription' => $changeDescription,
            ],
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
                $data['configuration'] = (object) [];
            } else {
                $data['configuration'] = $options->getConfiguration();
            }
        }

        if ($options->getState() !== null) {
            if ($options->getState() === []) {
                $data['state'] = (object) [];
            } else {
                $data['state'] = $options->getState();
            }
        }

        if ($options->getIsDisabled() !== null) {
            $data['isDisabled'] = $options->getIsDisabled();
        }

        if ($options->getChangeDescription()) {
            $data['changeDescription'] = $options->getChangeDescription();
        }

        return $this->client->apiPutJson(
            sprintf(
                $this->branchPrefix . 'components/%s/configs/%s/rows/%s',
                $options->getComponentConfiguration()->getComponentId(),
                $options->getComponentConfiguration()->getConfigurationId(),
                $options->getRowId(),
            ),
            $data,
        );
    }

    public function updateConfigurationRowState(ConfigurationRowState $options)
    {
        $data = [];

        if ($options->getState() !== null) {
            if ($options->getState() === []) {
                $data['state'] = (object) [];
            } else {
                $data['state'] = $options->getState();
            }
        }

        return $this->client->apiPutJson(
            sprintf(
                $this->branchPrefix . 'components/%s/configs/%s/rows/%s/state',
                $options->getComponentConfiguration()->getComponentId(),
                $options->getComponentConfiguration()->getConfigurationId(),
                $options->getRowId(),
            ),
            $data,
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
                http_build_query($options->toParamsArray()),
            ),
        );
    }

    public function getConfigurationRowVersion($componentId, $configurationId, $rowId, $version)
    {
        return $this->client->apiGet($this->branchPrefix . "components/{$componentId}/configs/{$configurationId}/rows/{$rowId}/versions/{$version}");
    }

    public function rollbackConfigurationRow($componentId, $configurationId, $rowId, $version, $changeDescription = null)
    {
        return $this->client->apiPostJson(
            $this->branchPrefix . "components/{$componentId}/configs/{$configurationId}/rows/{$rowId}/versions/{$version}/rollback",
            ['changeDescription' => $changeDescription],
        );
    }

    public function createConfigurationRowFromVersion($componentId, $configurationId, $rowId, $version, $targetConfigurationId = null, $changeDescription = null)
    {
        return $this->client->apiPostJson(
            $this->branchPrefix . "components/{$componentId}/configs/{$configurationId}/rows/{$rowId}/versions/{$version}/create",
            ['targetConfigId' => $targetConfigurationId, 'changeDescription' => $changeDescription],
        );
    }

    public function createConfigurationWorkspace($componentId, $configurationId, array $options = [], bool $async = false)
    {
        $url = $this->branchPrefix . "components/{$componentId}/configs/{$configurationId}/workspaces";
        if ($async) {
            $url .= '?' . http_build_query(['async' => true]);
        }

        return (new Workspaces($this->client))->decorateWorkspaceCreateWithCredentials(
            $options,
            function (array $options) use ($url) {
                $workspaceResponse = $this->client->apiPostJson(
                    $url,
                    $options,
                    true,
                    [Client::REQUEST_OPTION_EXTENDED_TIMEOUT => true],
                );
                assert(is_array($workspaceResponse));
                return $workspaceResponse;
            },
        );
    }

    public function addConfigurationMetadata(ConfigurationMetadata $options)
    {
        return $this->client->apiPostJson(
            sprintf(
                $this->branchPrefix . 'components/%s/configs/%s/metadata',
                $options->getComponentConfiguration()->getComponentId(),
                $options->getComponentConfiguration()->getConfigurationId(),
            ),
            [
                'metadata' => $options->getMetadata(),
            ],
        );
    }

    public function listConfigurationMetadata(ListConfigurationMetadataOptions $options)
    {
        return $this->client->apiGet(sprintf(
            $this->branchPrefix . 'components/%s/configs/%s/metadata',
            $options->getComponentId(),
            $options->getConfigurationId(),
        ));
    }

    public function deleteConfigurationMetadata($componentId, $configurationId, $metadataId)
    {
        return $this->client->apiDelete(sprintf(
            $this->branchPrefix . 'components/%s/configs/%s/metadata/%s',
            $componentId,
            $configurationId,
            $metadataId,
        ));
    }
}
