<?php

declare(strict_types=1);

namespace Keboola\Test\Backend\Snowflake;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Workspaces;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;
use Keboola\Test\Backend\Workspaces\WorkspacesTestCase;
use Keboola\Test\StorageApiTestCase;

class CloneInputMappingTest extends WorkspacesTestCase
{
    const IMPORT_FILE_PATH = __DIR__ . '/../../_data/languages.csv';

    public function testCloneInputMapping(): void
    {
        $bucketId = $this->ensureBucket($this->_client, 'clone-input-mapping');
        $tableId = $this->createTableFromFile(
            $this->_client,
            $bucketId,
            self::IMPORT_FILE_PATH
        );

        $this->runAndAssertWorkspaceClone($tableId);
    }

    public function testCloneSimpleAlias(): void
    {
        $bucketId = $this->ensureBucket($this->_client, 'clone-alias-bucket', 'in');
        $sourceTableId = $this->createTableFromFile(
            $this->_client,
            $bucketId,
            self::IMPORT_FILE_PATH
        );
        $bucketId = $this->ensureBucket($this->_client, 'clone-input-mapping');
        $tableId = $this->_client->createAliasTable($bucketId, $sourceTableId);

        $this->runAndAssertWorkspaceClone($tableId);
    }

    private function ensureBucket(
        Client $client,
        string $bucketName,
        string $stage = 'in'
    ): string {
        if ($client->bucketExists($stage . '.' . 'c-' . $bucketName)) {
            $client->dropBucket(
                $stage . '.' . 'c-' . $bucketName,
                [
                    'force' => true,
                ]
            );
        }
        return $client->createBucket($bucketName, $stage);
    }

    /**
     * @param array|string $primaryKey
     */
    private function createTableFromFile(
        Client $client,
        string $bucketId,
        string $importFilePath,
        $primaryKey = 'id'
    ): string {
        return $client->createTable(
            $bucketId,
            'languagesDetails',
            new CsvFile($importFilePath),
            ['primaryKey' => $primaryKey]
        );
    }

    /**
     * @param $tableId
     * @param $workspacesClient
     * @param $workspace
     */
    private function runAndAssertWorkspaceClone($tableId): void
    {
        $workspacesClient = new Workspaces($this->_client);

        $workspace = $workspacesClient->createWorkspace([
            'name' => 'clone',
        ]);

        $workspacesClient->cloneIntoWorkspace($workspace['id'], [
            'input' => [
                [
                    'source' => $tableId,
                    'destination' => 'languagesDetails',
                ],
            ],
        ]);

        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);
        $workspaceTableColumns = $backend->describeTableColumns('languagesDetails');
        $this->assertEquals(
            [
                [
                    'name' => 'id',
                    'type' => 'VARCHAR(1048576)',
                ],
                [
                    'name' => 'name',
                    'type' => 'VARCHAR(1048576)',
                ],
                [
                    'name' => '_timestamp',
                    'type' => 'TIMESTAMP_NTZ(9)',
                ]
            ],
            array_map(
                function(array $column) {
                    return [
                        'name' => $column['name'],
                        'type' => $column['type'],
                    ];
                },
                $workspaceTableColumns
            )
        );

        $workspaceTableData = $backend->fetchAll('languagesDetails');
        $this->assertCount(5, $workspaceTableData);
    }
}
