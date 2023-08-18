<?php

declare(strict_types=1);

namespace Keboola\Test\Backend\SOX;

use Generator;
use Keboola\Csv\CsvFile;
use Keboola\StorageApi\BranchAwareClient;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Options\FileUploadOptions;
use Keboola\StorageApi\Workspaces;
use Keboola\Test\Backend\Workspaces\Backend\BigqueryWorkspaceBackend;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;
use Keboola\Test\ClientProvider\ClientProvider;
use Throwable;

class SOXWorkspaceTest extends SOXWorkspaceTestCase
{
    /** @var BranchAwareClient|Client */
    private $testClient;

    public function setUp(): void
    {
        parent::setUp();
        // branch is prepared in the parent class
        $this->testClient = $this->clientProvider->createClientForCurrentTest(
            $this->getClientOptionsForToken($this->getProvidedData()[1]),
            true,
        );
    }

    /** @dataProvider provideBranch */
    public function testTableLoadAsView(): void
    {
        $bucketId = $this->getTestBucketId(self::STAGE_IN);

        $workspaceSapiClient = $this->workspaceSapiClient;
        assert($workspaceSapiClient instanceof BranchAwareClient);
        $workspaces = new Workspaces($workspaceSapiClient);
        $workspace = $this->initTestWorkspace($this->testClient, null, [], true);
        $backend = WorkspaceBackendFactory::createWorkspaceBackend($workspace, true);
        $importFile = __DIR__ . '/../../_data/languages.csv';
        $tableId = $this->testClient->createTableAsync(
            $bucketId,
            'languages',
            new CsvFile($importFile),
        );

        $options = [
            'input' => [
                [
                    'source' => $tableId,
                    'destination' => 'languages',
                    'sourceBranchId' => $workspaceSapiClient->getCurrentBranchId(),
                    'useView' => true,
                ],
            ],
        ];

        $workspaces->loadWorkspaceData($workspace['id'], $options);

        $tableRef = $backend->getTableReflection('languages');
        if (!$backend instanceof BigqueryWorkspaceBackend) { // todo: bq view reflection
            $viewRef = $backend->getViewReflection('languages');
            // View definition should be available
            //@phpstan-ignore-next-line  should be part of interface
            self::assertTrue($tableRef->isView());
            //@phpstan-ignore-next-line  should be part of interface
            self::assertStringStartsWith('CREATE VIEW', $viewRef->getViewDefinition());
            self::assertEquals(['id', 'name', '_timestamp'], $tableRef->getColumnsNames());
            self::assertCount(5, $backend->fetchAll('languages'));
        }

        // test if view select fail after column add
        $this->testClient->addTableColumn($tableId, 'newGuy');
        $tableRef = $backend->getTableReflection('languages');
        self::assertEquals(['id', 'name', '_timestamp'], $tableRef->getColumnsNames());

        if ($backend instanceof BigqueryWorkspaceBackend) {
            // in BQ view is just point and it will work after source table change
            $rows = $backend->fetchAll('languages');
            $this->assertCount(5, $rows);
        } else {
            try {
                $backend->fetchAll('languages');
                $this->fail('Must throw exception view columns mismatch');
            } catch (Throwable $e) {
                $this->assertStringContainsString('declared 3 column(s), but view query produces 4 column(s).', $e->getMessage());
            }
        }

        // test that doesn't work after column remove
        $this->testClient->deleteTableColumn($tableId, 'name');
        $tableRef = $backend->getTableReflection('languages');
        self::assertEquals(['id', 'name', '_timestamp'], $tableRef->getColumnsNames());
        if ($backend instanceof BigqueryWorkspaceBackend) {
            // in BQ view is just point and it will work after source table change
            $rows = $backend->fetchAll('languages');
            $this->assertCount(5, $rows);
        } else {
            try {
                $backend->fetchAll('languages');
                $this->fail('Must throw exception view columns mismatch');
            } catch (Throwable $e) {
                $this->assertStringContainsString('View columns mismatch with view definition for view \'languages\'', $e->getMessage());
            }
        }

        // overwrite view and test if it works
        $workspaces->loadWorkspaceData($workspace['id'], $options);
        $tableRef = $backend->getTableReflection('languages');
        self::assertEquals(['id', '_timestamp', 'newGuy'], $tableRef->getColumnsNames());
        self::assertCount(5, $backend->fetchAll('languages'));

        // run load without preserve to drop the view
        $options = [
            'input' => [
                [
                    'source' => $tableId,
                    'destination' => 'languages',
                    'sourceBranchId' => $workspaceSapiClient->getCurrentBranchId(),
                ],
            ],
        ];
        if ($backend instanceof BigqueryWorkspaceBackend) {
            // BQ doesn't support normal load so use view load to test if it works
            $options['input'][0]['useView'] = true;
        }
        $workspaces->loadWorkspaceData($workspace['id'], $options);
        if ($backend instanceof BigqueryWorkspaceBackend) {
            $backend->dropView('languages');
        } else {
            $backend->dropTable('languages');
        }

        $this->testClient->dropTable($tableId);
        $tableId = $this->testClient->createTableAsync(
            $bucketId,
            'languages',
            new CsvFile($importFile),
        );
        $workspaces->loadWorkspaceData($workspace['id'], $options);

        // test preserve load
        $options = [
            'input' => [
                [
                    'source' => $tableId,
                    'destination' => 'languages',
                    'sourceBranchId' => $workspaceSapiClient->getCurrentBranchId(),
                    'useView' => true,
                ],
            ],
            'preserve' => true,
        ];
        try {
            $workspaces->loadWorkspaceData($workspace['id'], $options);
            self::fail('Must throw exception view exists');
        } catch (ClientException $e) {
            self::assertEquals('Table languages already exists in workspace', $e->getMessage());
        }

        // test preserve load with overwrite
        $options = [
            'input' => [
                [
                    'source' => $tableId,
                    'destination' => 'languages',
                    'sourceBranchId' => $workspaceSapiClient->getCurrentBranchId(),
                    'useView' => true,
                    'overwrite' => true,
                ],
            ],
            'preserve' => true,
        ];
        $workspaces->loadWorkspaceData($workspace['id'], $options);
        $tableRef = $backend->getTableReflection('languages');
        self::assertEquals(['id', 'name', '_timestamp'], $tableRef->getColumnsNames());
        self::assertCount(5, $backend->fetchAll('languages'));

        // test workspace load incremental to view
        $options = [
            'input' => [
                [
                    'source' => $tableId,
                    'destination' => 'languages',
                    'incremental' => true,
                    'sourceBranchId' => $workspaceSapiClient->getCurrentBranchId(),
                    'useView' => false,
                ],
            ],
            'preserve' => true,
        ];
        try {
            $workspaces->loadWorkspaceData($workspace['id'], $options);
            self::fail('Incremental load to view cannot work.');
        } catch (ClientException $e) {
            if ($backend instanceof BigqueryWorkspaceBackend) {
                self::assertSame('Backend "bigquery" does not support: "Other types of loading than view".', $e->getMessage());
            } else {
                // this is expected edge case, view has also _timestamp col
                // which is ignored when validation incremental load
                // https://keboola.atlassian.net/browse/SOX-76
                self::assertStringStartsWith('Some columns are missing in source table', $e->getMessage());
            }
        }

        // do incremental load from file to source table
        $this->testClient->writeTableAsync(
            $tableId,
            new CsvFile($importFile),
            ['incremental' => true],
        );
        // test view is still working
        $tableRef = $backend->getTableReflection('languages');
        self::assertEquals(['id', 'name', '_timestamp'], $tableRef->getColumnsNames());
        self::assertCount(10, $backend->fetchAll('languages'));

        // load data from workspace to table
        $workspace2 = $workspaces->createWorkspace([], true);
        $options = [
            'input' => [
                [
                    'source' => $tableId,
                    'destination' => 'languages',
                    'sourceBranchId' => $workspaceSapiClient->getCurrentBranchId(),
                ],
            ],
        ];
        try {
            $workspaces->loadWorkspaceData($workspace2['id'], $options);
            $this->testClient->writeTableAsyncDirect(
                $tableId,
                [
                    'dataWorkspaceId' => $workspace2['id'],
                    'dataObject' => 'languages',
                ],
            );

            // test view is still working
            $tableRef = $backend->getTableReflection('languages');
            self::assertEquals(['id', 'name', '_timestamp'], $tableRef->getColumnsNames());
            self::assertCount(10, $backend->fetchAll('languages'));
            if ($backend instanceof BigqueryWorkspaceBackend) {
                $this->fail('BigQuery should throw exception');
            }
        } catch (ClientException $e) {
            if ($backend instanceof BigqueryWorkspaceBackend) {
                self::assertSame('Backend "bigquery" does not support: "Other types of loading than view".', $e->getMessage());
            } else {
                throw $e;
            }
        }

        // @phpstan-ignore-next-line
        if (self::TEST_FILE_WORKSPACE) {
            $fileId = $workspaceSapiClient->uploadFile(
                (new CsvFile($importFile))->getPathname(),
                (new FileUploadOptions())
                    ->setNotify(false)
                    ->setIsPublic(false)
                    ->setCompress(true)
                    ->setTags(['test-file-1']),
            );
            // load data from file workspace Not supported yet on S3 on ABS and not used in SNFLK
            $fileWorkspace = $workspaces->createWorkspace(
                [
                    'backend' => 'abs',
                ],
                true,
            );
            $options = [
                'input' => [
                    [
                        'dataFileId' => $fileId,
                        'destination' => 'languages',
                    ],
                ],
            ];
            $workspaces->loadWorkspaceData($fileWorkspace['id'], $options);
            $this->testClient->writeTableAsyncDirect(
                $tableId,
                [
                    'dataWorkspaceId' => $fileWorkspace['id'],
                    'dataObject' => 'languages/',
                ],
            );
            // test view is still working
            $tableRef = $backend->getTableReflection('languages');
            self::assertEquals(['id', 'name', '_timestamp'], $tableRef->getColumnsNames());
            $backend->fetchAll('languages');
            self::assertCount(5, $backend->fetchAll('languages'));
        }
        // test drop table
        $this->testClient->dropTable($tableId);
        $schemaRef = $backend->getSchemaReflection();
        self::assertCount(0, $schemaRef->getTablesNames());
        // view is still in workspace but not working
        self::assertCount(1, $schemaRef->getViewsNames());
        try {
            $backend->fetchAll('languages');
            $this->fail('View should not work after table drop');
        } catch (Throwable $e) {
            if ($backend instanceof BigqueryWorkspaceBackend) {
                self::assertStringContainsString('Not found: Table', $e->getMessage());
            } else {
                $this->assertStringContainsString('does not exist or not authorized', $e->getMessage());
            }
        }
    }

    public function provideBranch(): Generator
    {
        yield 'branch' => [
            ClientProvider::DEV_BRANCH,
            STORAGE_API_DEVELOPER_TOKEN,
        ];
    }
}
