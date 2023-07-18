<?php

namespace Keboola\Test\Backend\SOX;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\BranchAwareClient;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\DevBranches;
use Keboola\Test\StorageApiTestCase;
use Keboola\Test\Utils\EventsQueryBuilder;
use Keboola\Test\Utils\MetadataUtils;
use Symfony\Component\Filesystem\Filesystem;
use Throwable;

class BranchStorageTest extends StorageApiTestCase
{
    use MetadataUtils;

    private DevBranches $branches;

    public function setUp(): void
    {
        parent::setUp();
        $developerClient = $this->getDeveloperStorageApiClient();
        $this->branches = new DevBranches($developerClient);
        foreach ($this->getBranchesForCurrentTestCase($this->branches) as $branch) {
            try {
                $this->branches->deleteBranch($branch['id']);
            } catch (Throwable $e) {
                // ignore if delete fails
            }
        }
    }

    public function testDeleteTable(): void
    {
        [$privilegedClient, $productionTableId, $branchClient, $devTableId] = $this->initResources();

        // drop branch table
        $branchClient->dropTable($devTableId);
        $this->assertBranchEvent($branchClient, 'storage.tableDeleted', $devTableId, 'table');

        // check that production table still exists and we can preview data
        $privilegedClient->getTable($productionTableId);
        $privilegedClient->getTableDataPreview($productionTableId);

        try {
            $branchClient->getTable($devTableId);
            $this->fail('Table should not exist');
        } catch (ClientException $e) {
            $this->assertSame(404, $e->getCode());
        }

        // pull table again
        $devTableId = $branchClient->pullTableToBranch($productionTableId);

        // drop production table
        $privilegedClient->dropTable($productionTableId);
        $this->assertBranchEvent($privilegedClient, 'storage.tableDeleted', $productionTableId, 'table');

        // check that development table still exists and we can preview data
        $branchClient->getTable($devTableId);
        $branchClient->getTableDataPreview($devTableId);

        try {
            $privilegedClient->getTable($productionTableId);
            $this->fail('Table should not exist');
        } catch (ClientException $e) {
            $this->assertSame(404, $e->getCode());
        }
    }

    public function testTableImportExport(): void
    {
        [$privilegedClient, $productionTableId, $branchClient, $devTableId] = $this->initResources();
        $this->assertTableRowsCount(5, $devTableId, $branchClient);
        $this->assertTableRowsCount(5, $productionTableId, $privilegedClient);

        $branchClient->writeTableAsync(
            $devTableId,
            new CsvFile(__DIR__ . '/../../_data/languages.increment.csv'),
            [
                'incremental' => true,
            ]
        );
        $this->assertBranchEvent($branchClient, 'storage.tableImportDone', $devTableId, 'table');
        $this->assertTableRowsCount(8, $devTableId, $branchClient);
        $this->assertTableRowsCount(5, $productionTableId, $privilegedClient);

        $privilegedClient->writeTableAsync(
            $productionTableId,
            new CsvFile(__DIR__ . '/../../_data/languages.more-rows.csv'),
            [
                'incremental' => true,
            ]
        );
        $this->assertBranchEvent($privilegedClient, 'storage.tableImportDone', $productionTableId, 'table');
        $this->assertTableRowsCount(8, $devTableId, $branchClient);
        $this->assertTableRowsCount(12, $productionTableId, $privilegedClient);

        // test export
        $this->initEvents($privilegedClient);
        $this->initEvents($branchClient);

        $results = $branchClient->exportTableAsync($devTableId);
        $this->assertFileRowsCount(8, $results['file']['id'], $branchClient);
        $this->assertBranchEvent($branchClient, 'storage.tableExported', $devTableId, 'table');

        $results = $privilegedClient->exportTableAsync($productionTableId);
        $this->assertBranchEvent($privilegedClient, 'storage.tableExported', $productionTableId, 'table');
        $this->assertFileRowsCount(12, $results['file']['id'], $privilegedClient);
    }

    public function testTableColumnOperations(): void
    {
        [$privilegedClient, $productionTableId, $branchClient, $devTableId] = $this->initResources();
        $this->assertSame(['id', 'name'], $branchClient->getTable($devTableId)['columns']);
        $this->assertSame(['id', 'name'], $privilegedClient->getTable($productionTableId)['columns']);

        // add drop column on dev table
        $branchClient->addTableColumn($devTableId, 'colX');
        $this->assertBranchEvent($branchClient, 'storage.tableColumnAdded', $devTableId, 'table');
        $this->assertSame(['id', 'name', 'colX'], $branchClient->getTable($devTableId)['columns']);
        $this->assertSame(['id', 'name'], $privilegedClient->getTable($productionTableId)['columns']);

        $branchClient->deleteTableColumn($devTableId, 'colX');
        $this->assertBranchEvent($branchClient, 'storage.tableColumnDeleted', $devTableId, 'table');
        $this->assertSame(['id', 'name'], $branchClient->getTable($devTableId)['columns']);
        $this->assertSame(['id', 'name'], $privilegedClient->getTable($productionTableId)['columns']);

        // add drop column on prod table
        $privilegedClient->addTableColumn($productionTableId, 'colX');
        $this->assertBranchEvent($privilegedClient, 'storage.tableColumnAdded', $productionTableId, 'table');
        $this->assertSame(['id', 'name'], $branchClient->getTable($devTableId)['columns']);
        $this->assertSame(['id', 'name', 'colX'], $privilegedClient->getTable($productionTableId)['columns']);

        $privilegedClient->deleteTableColumn($productionTableId, 'colX');
        $this->assertBranchEvent($privilegedClient, 'storage.tableColumnDeleted', $productionTableId, 'table');
        $this->assertSame(['id', 'name'], $branchClient->getTable($devTableId)['columns']);
        $this->assertSame(['id', 'name'], $privilegedClient->getTable($productionTableId)['columns']);
    }


    public function testTablePrimaryKeyOperations(): void
    {
        [$privilegedClient, $productionTableId, $branchClient, $devTableId] = $this->initResources();

        $branchClient->createTablePrimaryKey($devTableId, ['id']);
        $this->assertBranchEvent($branchClient, 'storage.tablePrimaryKeyAdded', $devTableId, 'table');
        $this->assertSame(['id'], $branchClient->getTable($devTableId)['primaryKey']);
        $this->assertSame([], $privilegedClient->getTable($productionTableId)['primaryKey']);

        $branchClient->removeTablePrimaryKey($devTableId);
        $this->assertBranchEvent($branchClient, 'storage.tablePrimaryKeyDeleted', $devTableId, 'table');
        $this->assertSame([], $branchClient->getTable($devTableId)['primaryKey']);
        $this->assertSame([], $privilegedClient->getTable($productionTableId)['primaryKey']);

        // add drop primary key on prod table
        $privilegedClient->createTablePrimaryKey($productionTableId, ['id']);
        $this->assertBranchEvent($privilegedClient, 'storage.tablePrimaryKeyAdded', $productionTableId, 'table');
        $this->assertSame([], $branchClient->getTable($devTableId)['primaryKey']);
        $this->assertSame(['id'], $privilegedClient->getTable($productionTableId)['primaryKey']);

        $privilegedClient->removeTablePrimaryKey($productionTableId);
        $this->assertBranchEvent($privilegedClient, 'storage.tablePrimaryKeyDeleted', $productionTableId, 'table');
        $this->assertSame([], $branchClient->getTable($devTableId)['primaryKey']);
        $this->assertSame([], $privilegedClient->getTable($productionTableId)['primaryKey']);
    }


    public function testTableDeleteRowsOperations(): void
    {
        [$privilegedClient, $productionTableId, $branchClient, $devTableId] = $this->initResources();
        $branchClient->deleteTableRows($devTableId, [
            'whereColumn' => 'id',
            'whereValues' => ['1'],
        ]);
        $this->assertBranchEvent($branchClient, 'storage.tableRowsDeleted', $devTableId, 'table');
        $this->assertTableRowsCount(4, $devTableId, $branchClient);
        $this->assertTableRowsCount(5, $productionTableId, $privilegedClient);

        $privilegedClient->deleteTableRows($productionTableId, [
            'whereColumn' => 'id',
            'whereValues' => ['11'],
        ]);
        $this->assertBranchEvent($privilegedClient, 'storage.tableRowsDeleted', $productionTableId, 'table');
        $this->assertTableRowsCount(4, $devTableId, $branchClient);
        $this->assertTableRowsCount(4, $productionTableId, $privilegedClient);
    }

    public function testTableUpdateOperation(): void
    {
        [$privilegedClient, $productionTableId, $branchClient, $devTableId] = $this->initResources();
        $branchClient->updateTable($devTableId, [
            'displayName' => 'new_name',
        ]);
        $this->assertBranchEvent($branchClient, 'storage.tableUpdated', $devTableId, 'table');
        $this->assertSame('new_name', $branchClient->getTable($devTableId)['displayName']);
        $this->assertSame('languages', $privilegedClient->getTable($productionTableId)['displayName']);

        $privilegedClient->updateTable($productionTableId, [
            'displayName' => 'new_prod',
        ]);
        $this->assertBranchEvent($privilegedClient, 'storage.tableUpdated', $productionTableId, 'table');
        $this->assertSame('new_name', $branchClient->getTable($devTableId)['displayName']);
        $this->assertSame('new_prod', $privilegedClient->getTable($productionTableId)['displayName']);
    }

    public function testTableSnapshot(): void
    {
        [$privilegedClient, $productionTableId, $branchClient, $devTableId] = $this->initResources();
        // delete rows so tables are different
        $privilegedClient->deleteTableRows($productionTableId, [
            'whereColumn' => 'id',
            'whereValues' => ['11'],
        ]);
        $prodTable = $privilegedClient->getTable($productionTableId);
        $devTable = $branchClient->getTable($devTableId);

        $snapshotDev = $branchClient->createTableSnapshot($devTableId);
        $this->assertBranchEvent($branchClient, 'storage.tableSnapshotCreated', $devTableId, 'table');
        $newDevTableId = $branchClient->createTableFromSnapshot($devTable['bucket']['id'], $snapshotDev, 'new-table');
        $this->assertBranchEvent($branchClient, 'storage.tableCreated', $newDevTableId, 'table');
        $this->assertTableRowsCount(5, $newDevTableId, $branchClient);

        $snapshotProd = $privilegedClient->createTableSnapshot($productionTableId);
        $this->assertBranchEvent($privilegedClient, 'storage.tableSnapshotCreated', $productionTableId, 'table');
        $newProductionTableId = $privilegedClient->createTableFromSnapshot($prodTable['bucket']['id'], $snapshotProd, 'new-table');
        $this->assertBranchEvent($privilegedClient, 'storage.tableCreated', $newProductionTableId, 'table');
        $this->assertTableRowsCount(4, $newProductionTableId, $privilegedClient);

        // try to create dev branch table from production snapshot
        $newDevTableId = $branchClient->createTableFromSnapshot($devTable['bucket']['id'], $snapshotProd, 'new-table-prod');
        $this->assertBranchEvent($branchClient, 'storage.tableCreated', $newDevTableId, 'table');
        $this->assertTableRowsCount(4, $newDevTableId, $branchClient);
    }

    public function testListingTablesBuckets(): void
    {
        [$privilegedClient, $productionTableId, $branchClient, $devTableId] = $this->initResources();
        $prodTable = $privilegedClient->getTable($productionTableId);
        $this->assertBranchEvent($privilegedClient, 'storage.tableDetail', $productionTableId, 'table');
        $devTable = $branchClient->getTable($devTableId);
        $this->assertBranchEvent($branchClient, 'storage.tableDetail', $devTableId, 'table');
        // check that table is and bucket id are same in prod and dev
        $this->assertSame($prodTable['bucket']['id'], $devTable['bucket']['id']);
        $this->assertSame($prodTable['id'], $devTable['id']);

        // assert tables listing
        $tablesInProd = array_filter(
            $privilegedClient->listTables(),
            fn(array $table) => $table['bucket']['id'] === $prodTable['bucket']['id'] && $table['id'] === $productionTableId
        );
        $this->assertBranchEvent($privilegedClient, 'storage.tablesListed', null, null);
        $this->assertCount(1, $tablesInProd);
        $this->assertCount(1, $branchClient->listTables());
        $this->assertBranchEvent($branchClient, 'storage.tablesListed', null, null);
        // assert buckets listing
        $bucketsInProd = array_filter(
            $privilegedClient->listBuckets(),
            fn(array $bucket) => $bucket['id'] === $prodTable['bucket']['id']
        );
        $this->assertBranchEvent($privilegedClient, 'storage.bucketsListed', null, null);
        $this->assertCount(1, $bucketsInProd);
        $this->assertCount(1, $branchClient->listBuckets());
        $this->assertBranchEvent($branchClient, 'storage.bucketsListed', null, null);
    }

    /**
     * @return array{Client, string, BranchAwareClient, string}
     */
    private function initResources(): array
    {
        $description = $this->generateDescriptionForTestObject();
        $newBranch = $this->branches->createBranch($this->generateDescriptionForTestObject());

        $privilegedClient = $this->getDefaultBranchStorageApiClient();
        $productionBucketId = $this->initEmptyBucket(
            $this->getTestBucketName($description),
            self::STAGE_IN,
            $description,
            $privilegedClient
        );
        $productionTableId = $privilegedClient->createTableAsync(
            $productionBucketId,
            'languages',
            new CsvFile(__DIR__ . '/../../_data/languages.csv')
        );
        $branchClient = $this->getBranchAwareClient($newBranch['id'], [
            'token' => STORAGE_API_DEVELOPER_TOKEN,
            'url' => STORAGE_API_URL,
        ]);
        $devTableId = $branchClient->pullTableToBranch($productionTableId);
        $this->initEvents($privilegedClient);
        $this->initEvents($branchClient);

        return [$privilegedClient, $productionTableId, $branchClient, $devTableId];
    }

    private function assertTableRowsCount(int $expectedRows, string $tableId, Client $client): void
    {
        $this->assertCount($expectedRows, Client::parseCsv($client->getTableDataPreview($tableId)));
    }

    private function assertFileRowsCount(int $expectedRows, int $fileId, Client $client): void
    {
        $tmpDestinationFolder = __DIR__ . '/../_tmp/branch-storage-export/';
        $fs = new Filesystem();
        if (file_exists($tmpDestinationFolder)) {
            $fs->remove($tmpDestinationFolder);
        }
        $fs->mkdir($tmpDestinationFolder);
        $slices = $client->downloadSlicedFile($fileId, $tmpDestinationFolder);

        $csv = '';
        foreach ($slices as $slice) {
            $csv .= file_get_contents($slice);
        }

        $parsedData = Client::parseCsv($csv, false, ',', '"');
        $this->assertCount($expectedRows, $parsedData);
    }

    private function assertBranchEvent(Client $client, string $eventName, ?string $objectId, ?string $objectType): void
    {
        $eventsQuery = new EventsQueryBuilder();
        $eventsQuery->setEvent($eventName);
        if ($objectId !== null) {
            $eventsQuery->setObjectId($objectId);
        }
        if ($objectType !== null) {
            $eventsQuery->setObjectType($objectType);
        }
        // expect only one drop table event
        $assertEventCallback = function ($events) use ($eventName) {
            $this->assertCount(1, $events);
            $this->assertSame($events[0]['event'], $eventName);
        };

        $this->assertEventWithRetries($client, $assertEventCallback, $eventsQuery);
    }
}
