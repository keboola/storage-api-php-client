<?php

namespace Keboola\Test\Backend\Mixed\Workspaces;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Workspaces;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;
use Keboola\Test\Backend\Workspaces\WorkspacesTestCase;

abstract class BaseWorkSpacesTestCase extends WorkspacesTestCase
{
    abstract public function workspaceBackendData();

    abstract public function workspaceMixedAndSameBackendDataWithDataTypes();

    abstract public function workspaceMixedAndSameBackendData();

    /**
     * @dataProvider  workspaceBackendData
     * @param $backend
     * @param $dataTypesDefinition
     */
    public function testCreateWorkspaceParam($backend, $dataTypesDefinition): void
    {
        $workspaces = new Workspaces($this->_client);

        $workspace = $workspaces->createWorkspace(
            [
                'backend' => $backend,
            ],
            true,
        );
        $this->assertEquals($backend, $workspace['connection']['backend']);

        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace);

        $backend->createTable('mytable', $dataTypesDefinition);
    }

    /**
     * @dataProvider workspaceMixedAndSameBackendDataWithDataTypes
     * @param $workspaceBackend
     * @param $sourceBackend
     * @param $columnsDefinition
     */
    public function testLoadUserError($workspaceBackend, $sourceBackend, $columnsDefinition): void
    {
        if ($this->_client->bucketExists('in.c-mixed-test-' . $sourceBackend)) {
            $this->_client->dropBucket("in.c-mixed-test-{$sourceBackend}", [
                'force' => true,
                'async' => true,
            ]);
        }
        $bucketId = $this->_client->createBucket("mixed-test-{$sourceBackend}", 'in', '', $sourceBackend);
        $sourceTableId = $this->_client->createTableAsync(
            $bucketId,
            'transactions',
            new CsvFile(__DIR__ . '/../../../_data/transactions.csv'),
        );

        $workspaces = new Workspaces($this->_client);

        $workspace = $workspaces->createWorkspace(
            [
                'backend' => $workspaceBackend,
            ],
            true,
        );

        $options = [
            'input' => [
                [
                    'source' => $sourceTableId,
                    'destination' => 'transactions',
                    'columns' => $columnsDefinition,
                ],
            ],
        ];

        // exception should be thrown, as quantity has empty value '' and snflk will complain.
        try {
            $workspaces->loadWorkspaceData($workspace['id'], $options);
        } catch (ClientException $e) {
            $this->assertEquals($e->getStringCode(), 'workspace.tableLoad');
        }
    }

    /**
     * @dataProvider workspaceMixedAndSameBackendData
     * @param $workspaceBackend
     * @param $sourceBackend
     */
    public function testLoadWorkspaceExtendedDataTypesNullify($workspaceBackend, $sourceBackend): void
    {
        if ($this->_client->bucketExists('in.c-mixed-test-' . $sourceBackend)) {
            $this->_client->dropBucket("in.c-mixed-test-{$sourceBackend}", [
                'force' => true,
                'async' => true,
            ]);
        }
        $bucketId = $this->_client->createBucket("mixed-test-{$sourceBackend}", 'in', '', $sourceBackend);
        $sourceTableId = $this->_client->createTableAsync(
            $bucketId,
            'transactions',
            new CsvFile(__DIR__ . '/../../../_data/transactions-nullify.csv'),
        );

        $workspaces = new Workspaces($this->_client);

        $workspace = $workspaces->createWorkspace(
            [
                'backend' => $workspaceBackend,
            ],
            true,
        );

        $dataType = $workspaceBackend === self::BACKEND_SNOWFLAKE ? 'NUMBER' : 'INTEGER';
        $options = [
            'input' => [
                [
                    'source' => $sourceTableId,
                    'destination' => 'transactions',
                    'columns' => [
                        [
                            'source' => 'item',
                            'type' => 'VARCHAR',
                            'convertEmptyValuesToNull' => true,
                        ],
                        [
                            'source' => 'quantity',
                            'type' => $dataType,
                            'convertEmptyValuesToNull' => true,
                        ],
                    ],
                ],
            ],
        ];
        $workspaces->loadWorkspaceData($workspace['id'], $options);

        $workspaceBackendConnection = WorkspaceBackendFactory::createWorkspaceBackend($workspace);
        $data = $workspaceBackendConnection->fetchAll('transactions', \PDO::FETCH_ASSOC);
        $this->assertArrayHasKey('quantity', $data[0]);
        $this->assertArrayHasKey('item', $data[0]);
        $this->assertEquals(null, $data[0]['quantity']);
        $this->assertEquals(null, $data[0]['item']);
    }
}
