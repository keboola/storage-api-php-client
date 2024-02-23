<?php

namespace Keboola\Test\Backend\Exasol\Workspace;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Workspaces;
use Keboola\Test\Backend\Workspaces\Backend\InputMappingConverter;
use Keboola\Test\Backend\Workspaces\WorkspacesLoadTest;

class WorkspaceLoadTest extends WorkspacesLoadTest
{
    public function testDottedDestination(): void
    {
        $workspaces = new Workspaces($this->workspaceSapiClient);
        $workspace = $workspaces->createWorkspace([], true);

        // Create a table of sample data
        $importFile = __DIR__ . '/../../../_data/languages.csv';
        $tableId = $this->_client->createTableAsync(
            $this->getTestBucketId(self::STAGE_IN),
            'languages_dotted',
            new CsvFile($importFile),
        );

        $options = [
            'input' => [
                [
                    'source' => $tableId,
                    'destination' => 'dotted.destination',
                    'columns' => [
                        [
                            'source' => 'id',
                            'type' => 'INTEGER',
                        ],
                    ],
                ],
            ],
        ];

        $options = InputMappingConverter::convertInputColumnsTypesForBackend(
            $workspace['connection']['backend'],
            $options,
        );

        try {
            $workspaces->loadWorkspaceData($workspace['id'], $options);
            self::fail('Dotted destination is not supported in exasol');
        } catch (ClientException $e) {
            self::assertEquals(
                'Invalid table name: \'dotted.destination\' contains not allowed characters. Only alphanumeric characters dash and underscores are allowed.',
                $e->getMessage(),
            );
            self::assertEquals('workspace.loadRequestBadInput', $e->getStringCode());
        }
    }
}
