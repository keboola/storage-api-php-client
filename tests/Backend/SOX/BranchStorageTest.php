<?php

namespace Keboola\Test\Backend\SOX;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\BranchAwareClient;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\DevBranches;
use Keboola\Test\StorageApiTestCase;
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
        $privilegedClient->dropTable($devTableId);

        // check that development table still exists and we can preview data
        $branchClient->getTable($productionTableId);
        $branchClient->getTableDataPreview($productionTableId);

        try {
            $privilegedClient->getTable($devTableId);
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
        $this->assertTableRowsCount(8, $devTableId, $branchClient);
        $this->assertTableRowsCount(5, $productionTableId, $privilegedClient);

        $privilegedClient->writeTableAsync(
            $productionTableId,
            new CsvFile(__DIR__ . '/../../_data/languages.more-rows.csv'),
            [
                'incremental' => true,
            ]
        );
        $this->assertTableRowsCount(8, $devTableId, $branchClient);
        $this->assertTableRowsCount(12, $productionTableId, $privilegedClient);

        // test export
        $results = $branchClient->exportTableAsync($devTableId);
        // @todo SOX-20 will save file to branch storage and we can use $branchClient but for now we need to use $privilegedClient
        $this->assertFileRowsCount(8, $results['file']['id'], $privilegedClient);

        $results = $privilegedClient->exportTableAsync($productionTableId);
        $this->assertFileRowsCount(12, $results['file']['id'], $privilegedClient);
    }

    public function testTableColumnOperations(): void
    {
        [$privilegedClient, $productionTableId, $branchClient, $devTableId] = $this->initResources();
        $this->assertSame(['id', 'name'], $branchClient->getTable($devTableId)['columns']);
        $this->assertSame(['id', 'name'], $privilegedClient->getTable($productionTableId)['columns']);

        // add drop column on dev table
        $branchClient->addTableColumn($devTableId, 'colX');
        $this->assertSame(['id', 'name', 'colX'], $branchClient->getTable($devTableId)['columns']);
        $this->assertSame(['id', 'name'], $privilegedClient->getTable($productionTableId)['columns']);

        $branchClient->deleteTableColumn($devTableId, 'colX');
        $this->assertSame(['id', 'name'], $branchClient->getTable($devTableId)['columns']);
        $this->assertSame(['id', 'name'], $privilegedClient->getTable($productionTableId)['columns']);

        // add drop column on prod table
        $privilegedClient->addTableColumn($productionTableId, 'colX');
        $this->assertSame(['id', 'name'], $branchClient->getTable($devTableId)['columns']);
        $this->assertSame(['id', 'name', 'colX'], $privilegedClient->getTable($productionTableId)['columns']);

        $privilegedClient->deleteTableColumn($productionTableId, 'colX');
        $this->assertSame(['id', 'name'], $branchClient->getTable($devTableId)['columns']);
        $this->assertSame(['id', 'name'], $privilegedClient->getTable($productionTableId)['columns']);
    }


    public function testTablePrimaryKeyOperations(): void
    {
        [$privilegedClient, $productionTableId, $branchClient, $devTableId] = $this->initResources();
        $branchClient->createTablePrimaryKey($devTableId, ['id']);
        $this->assertSame(['id'], $branchClient->getTable($devTableId)['primaryKey']);
        $this->assertSame([], $privilegedClient->getTable($productionTableId)['primaryKey']);

        $branchClient->removeTablePrimaryKey($devTableId);
        $this->assertSame([], $branchClient->getTable($devTableId)['primaryKey']);
        $this->assertSame([], $privilegedClient->getTable($productionTableId)['primaryKey']);

        // add drop primary key on prod table
        $privilegedClient->createTablePrimaryKey($productionTableId, ['id']);
        $this->assertSame([], $branchClient->getTable($devTableId)['primaryKey']);
        $this->assertSame(['id'], $privilegedClient->getTable($productionTableId)['primaryKey']);

        $privilegedClient->removeTablePrimaryKey($productionTableId);
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
        $this->assertTableRowsCount(4, $devTableId, $branchClient);
        $this->assertTableRowsCount(5, $productionTableId, $privilegedClient);

        $privilegedClient->deleteTableRows($productionTableId, [
            'whereColumn' => 'id',
            'whereValues' => ['11'],
        ]);
        $this->assertTableRowsCount(4, $devTableId, $branchClient);
        $this->assertTableRowsCount(4, $productionTableId, $privilegedClient);
    }

    public function testTableUpdateOperation(): void
    {
        [$privilegedClient, $productionTableId, $branchClient, $devTableId] = $this->initResources();
        $branchClient->updateTable($devTableId, [
            'displayName' => 'new_name',
        ]);
        $this->assertSame('new_name', $branchClient->getTable($devTableId)['displayName']);
        $this->assertSame('languages', $privilegedClient->getTable($productionTableId)['displayName']);

        $privilegedClient->updateTable($productionTableId, [
            'displayName' => 'new_prod',
        ]);
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
        $newDevTableId = $branchClient->createTableFromSnapshot($devTable['bucket']['id'], $snapshotDev, 'new-table');
        $this->assertTableRowsCount(5, $newDevTableId, $branchClient);

        $snapshotProd = $privilegedClient->createTableSnapshot($productionTableId);
        $newProductionTableId = $privilegedClient->createTableFromSnapshot($prodTable['bucket']['id'], $snapshotProd, 'new-table');
        $this->assertTableRowsCount(4, $newProductionTableId, $privilegedClient);

        // try to create dev branch table from production snapshot
        $newDevTableId = $branchClient->createTableFromSnapshot($devTable['bucket']['id'], $snapshotProd, 'new-table-prod');
        $this->assertTableRowsCount(4, $newDevTableId, $branchClient);
    }

    public function testListingTablesBuckets(): void
    {
        [$privilegedClient, $productionTableId, $branchClient, $devTableId] = $this->initResources();
        $prodTable = $privilegedClient->getTable($productionTableId);
        $devTable = $branchClient->getTable($devTableId);
        // check that table is and bucket id are same in prod and dev
        $this->assertSame($prodTable['bucket']['id'], $devTable['bucket']['id']);
        $this->assertSame($prodTable['id'], $devTable['id']);
        // assert tables listing
        $tablesInProd = array_filter(
            $privilegedClient->listTables(),
            fn(array $table) => $table['bucket']['id'] === $prodTable['bucket']['id'] && $table['id'] === $productionTableId
        );
        $this->assertCount(1, $tablesInProd);
        $this->assertCount(1, $branchClient->listTables());
        // assert buckets listing
        $bucketsInProd = array_filter(
            $privilegedClient->listBuckets(),
            fn(array $bucket) => $bucket['id'] === $prodTable['bucket']['id']
        );
        $this->assertCount(1, $bucketsInProd);
        $this->assertCount(1, $branchClient->listBuckets());
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
}
