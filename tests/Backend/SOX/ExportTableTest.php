<?php

declare(strict_types=1);

namespace Keboola\Test\Backend\SOX;

use Keboola\Csv\CsvFile;
use Keboola\Csv\InvalidArgumentException;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\DevBranches;
use Keboola\StorageApi\TableExporter;
use Keboola\Test\StorageApiTestCase;

class ExportTableTest extends StorageApiTestCase
{
    private Client $developerClient;

    private string $downloadPath;

    private DevBranches $branches;

    public function setUp(): void
    {
        parent::setUp();
        $this->developerClient = $this->getDeveloperStorageApiClient();
        $this->branches = new DevBranches($this->developerClient);
        $this->cleanupTestBranches($this->developerClient);
        $this->downloadPath = $this->getExportFilePathForTest('languages.sliced.csv');
    }

    public function testTableAsyncExportInDevBranch(): void
    {
        [$importFile, $expectationsFileName] = $this->tableExportData();
        $expectationsFile = __DIR__ . '/../../_data/' . $expectationsFileName;

        $description = $this->generateDescriptionForTestObject();

        $newBranch = $this->branches->createBranch($this->generateDescriptionForTestObject() . '_aaaa');

        $developerDevBranchClient = $this->getBranchAwareClient($newBranch['id'], [
            'token' => STORAGE_API_DEVELOPER_TOKEN,
            'url' => STORAGE_API_URL,
        ]);

        $bucketName = $this->getTestBucketName($description);
        $devBranchBucketId = $this->initEmptyBucket(
            $bucketName,
            self::STAGE_IN,
            $description,
            $developerDevBranchClient
        );

        $tableId = $developerDevBranchClient->createTableAsync($devBranchBucketId, 'languages', $importFile);

        // test developer can export dev table in dev branch (dev table -> dev file)
        $developerDevBranchExporter = new TableExporter($developerDevBranchClient);
        $developerDevBranchExporter->exportTable($tableId, $this->downloadPath, []);

        // compare data
        $this->assertTrue(file_exists($this->downloadPath));
        $this->assertLinesEqualsSorted(file_get_contents($expectationsFile), file_get_contents($this->downloadPath), 'imported data comparison');

        $projectManagerDevBranchClient = $this->getBranchAwareClient($newBranch['id'], [
            'token' => STORAGE_API_DEFAULT_BRANCH_TOKEN,
            'url' => STORAGE_API_URL,
        ]);

        $projectManagerDevBranchExporter = new TableExporter($projectManagerDevBranchClient);
        try {
            $projectManagerDevBranchExporter->exportTable($tableId, $this->downloadPath, []);
            $this->fail('Project manager cannot export from table in devBranch');
        } catch (ClientException $e) {
            $this->assertStringContainsString(
                'You don\'t have access to the resource.',
                $e->getMessage()
            );
        }

        $defaultBranch = $this->branches->getDefaultBranch();
        $developerDefaultBranchClient = $this->getBranchAwareClient($defaultBranch['id'], [
            'token' => STORAGE_API_DEVELOPER_TOKEN,
            'url' => STORAGE_API_URL,
        ]);

        $developerDefaultBranchExporter = new TableExporter($developerDefaultBranchClient);

        // Test developer cannot export dev table using default branch file storage (dev table -> default file)
        try {
            $developerDefaultBranchExporter->exportTable($tableId, $this->downloadPath, []);
            $this->fail('Cannot export from table in devBranch via default branch client');
        } catch (ClientException $e) {
            $this->assertStringContainsString(
                sprintf('The table "languages" was not found in the bucket "%s" in the project ', $devBranchBucketId),
                $e->getMessage()
            );
        }
    }

    public function testTableAsyncExportInDefaultBranch(): void
    {
        [$importFile, $expectationsFileName] = $this->tableExportData();
        $expectationsFile = __DIR__ . '/../../_data/' . $expectationsFileName;

        $description = $this->generateDescriptionForTestObject();
        $projectManagerDefaultBranchClient = $this->getDefaultBranchStorageApiClient();

        $bucketName = $this->getTestBucketName($description);
        $productionBucketId = $this->initEmptyBucket(
            $bucketName,
            self::STAGE_IN,
            $description,
            $projectManagerDefaultBranchClient
        );

        $tableId = $projectManagerDefaultBranchClient->createTableAsync($productionBucketId, 'languages', $importFile);

        // test project manager can export prod table in default branch (prod table -> prod file)
        $projectManagerDefaultBranchExporter = new TableExporter($projectManagerDefaultBranchClient);
        $projectManagerDefaultBranchExporter->exportTable($tableId, $this->downloadPath, []);

        // compare data
        $this->assertTrue(file_exists($this->downloadPath));
        $this->assertLinesEqualsSorted(file_get_contents($expectationsFile), file_get_contents($this->downloadPath), 'imported data comparison');

        $newBranch = $this->branches->createBranch($this->generateDescriptionForTestObject() . '_aaaa');
        $projectManagerDevBranchBranchClient = $this->getBranchAwareClient($newBranch['id'], [
            'token' => STORAGE_API_DEFAULT_BRANCH_TOKEN,
            'url' => STORAGE_API_URL,
        ]);

        $devBranchExporter = new TableExporter($projectManagerDevBranchBranchClient);

        try {
            $devBranchExporter->exportTable($tableId, $this->downloadPath, []);
            $this->fail('Cannot export from table in default branch via dev branch client');
        } catch (ClientException $e) {
            $this->assertSame(
                'You don\'t have access to the resource.',
                $e->getMessage()
            );
        }

        $defaultBranch = $this->branches->getDefaultBranch();
        $developerDefaultBranchClient = $this->getBranchAwareClient($defaultBranch['id'], [
            'token' => STORAGE_API_DEVELOPER_TOKEN,
            'url' => STORAGE_API_URL,
        ]);

        $developerDefaultBranchExporter = new TableExporter($developerDefaultBranchClient);

        try {
            $developerDefaultBranchExporter->exportTable($tableId, $this->downloadPath, []);
            $this->fail('Developer cannot export from table in default branch');
        } catch (ClientException $e) {
            $this->assertStringContainsString(
                'You don\'t have access to the resource.',
                $e->getMessage()
            );
        }
    }

    public function testExportTableExistInDefaultOnly(): void
    {
        [$importFile, $expectationsFileName] = $this->tableExportData();
        $expectationsFile = __DIR__ . '/../../_data/' . $expectationsFileName;

        $description = $this->generateDescriptionForTestObject();
        $projectManagerDefaultBranchClient = $this->getDefaultBranchStorageApiClient();

        $bucketName = $this->getTestBucketName($description);
        $productionBucketId = $this->initEmptyBucket(
            $bucketName,
            self::STAGE_IN,
            $description,
            $projectManagerDefaultBranchClient
        );

        $tableIdInDefault = $projectManagerDefaultBranchClient->createTableAsync($productionBucketId, 'languages', $importFile);

        $newBranch = $this->branches->createBranch($this->generateDescriptionForTestObject() . '_devBranch');

        $developerDevBranchBranchClient = $this->getBranchAwareClient($newBranch['id'], [
            'token' => STORAGE_API_DEVELOPER_TOKEN,
            'url' => STORAGE_API_URL,
        ]);

        // check that there is no table in the default branch
        try {
            $developerDevBranchBranchClient->getTable($tableIdInDefault);
            $this->fail('Table should not exist in dev branch');
        } catch (ClientException $e) {
            $this->assertStringContainsString(
                sprintf('The table "%s" was not found in the bucket "%s" in the project ', 'languages', $productionBucketId),
                $e->getMessage()
            );
        }
        $devBranchExporter = new TableExporter($developerDevBranchBranchClient);

        // try export prod table -> dev file, without `sourceBranchId` param
        try {
            $devBranchExporter->exportTable($tableIdInDefault, $this->downloadPath, []);
            $this->fail('Table not exist in dev branch and sourceBranchId is not set.');
        } catch (ClientException $e) {
            $this->assertStringContainsString(
                sprintf('The table "languages" was not found in the bucket "%s" in the project ', $productionBucketId),
                $e->getMessage()
            );
        }

        // try export prod table -> dev file, with `sourceBranchId` param
        $defaultBranch = $this->branches->getDefaultBranch();
        $devBranchExporter->exportTable($tableIdInDefault, $this->downloadPath, ['sourceBranchId' => $defaultBranch['id']]);

        // compare data
        $this->assertTrue(file_exists($this->downloadPath));
        $this->assertLinesEqualsSorted(file_get_contents($expectationsFile), file_get_contents($this->downloadPath), 'imported data comparison');

        // export prod table -> prod file with `sourceBranchId` param = prod branch
        // runner will always send this parameter
        $defaultBranchExporter = new TableExporter($projectManagerDefaultBranchClient);
        $defaultBranchExporter->exportTable($tableIdInDefault, $this->downloadPath, ['sourceBranchId' => $defaultBranch['id']]);
        // compare data
        $this->assertTrue(file_exists($this->downloadPath));
        $this->assertLinesEqualsSorted(file_get_contents($expectationsFile), file_get_contents($this->downloadPath), 'imported data comparison');
    }

    public function testExportTableExistInDevBranchOnly(): void
    {
        [$importFile, $expectationsFileName] = $this->tableExportData();
        $expectationsFile = __DIR__ . '/../../_data/' . $expectationsFileName;

        $description = $this->generateDescriptionForTestObject();

        $newBranch = $this->branches->createBranch($description . '_devBranch');

        $developerDevBranchBranchClient = $this->getBranchAwareClient($newBranch['id'], [
            'token' => STORAGE_API_DEVELOPER_TOKEN,
            'url' => STORAGE_API_URL,
        ]);

        $bucketName = $this->getTestBucketName($description);
        $devBranchBucketId = $this->initEmptyBucket(
            $bucketName,
            self::STAGE_IN,
            $description,
            $developerDevBranchBranchClient
        );

        $tableIdInDevBranch = $developerDevBranchBranchClient->createTableAsync($devBranchBucketId, 'languages', $importFile);

        $devBranchExporter = new TableExporter($developerDevBranchBranchClient);
        $devBranchExporter->exportTable($tableIdInDevBranch, $this->downloadPath, ['sourceBranchId' => $newBranch['id']]);
        // compare data
        $this->assertTrue(file_exists($this->downloadPath));
        $this->assertLinesEqualsSorted(file_get_contents($expectationsFile), file_get_contents($this->downloadPath), 'imported data comparison');

        $defaultBranch = $this->branches->getDefaultBranch();
        $projectManagerDefaultBranchClient = $this->getBranchAwareClient($defaultBranch['id'], [
            'token' => STORAGE_API_DEFAULT_BRANCH_TOKEN,
            'url' => STORAGE_API_URL,
        ]);
        try {
            $projectManagerDefaultBranchClient->getTable($tableIdInDevBranch);
            $this->fail('Table should not exist in default branch');
        } catch (ClientException $e) {
            $this->assertStringContainsString(
                sprintf('The table "%s" was not found in the bucket "%s" in the project ', 'languages', $devBranchBucketId),
                $e->getMessage()
            );
        }

        $defaultBranchExporter = new TableExporter($projectManagerDefaultBranchClient);

        // try export dev table -> prod file, without `sourceBranchId` param
        try {
            $defaultBranchExporter->exportTable($tableIdInDevBranch, $this->downloadPath, []);
            $this->fail('Table not exist in dev branch and sourceBranchId is not set.');
        } catch (ClientException $e) {
            $this->assertStringContainsString(
                sprintf('The table "languages" was not found in the bucket "%s" in the project ', $devBranchBucketId),
                $e->getMessage()
            );
        }

        // try export dev table -> prod file, with `sourceBranchId` param
        // api export endpoint supports this but the client does not support this yet
        try {
            $defaultBranchExporter->exportTable($tableIdInDevBranch, $this->downloadPath, ['sourceBranchId' => $newBranch['id']]);
            $this->fail('It is not implemented in the client, the api endpoint supports it but the client cannot get information about the bucket');
        } catch (ClientException $e) {
            $this->assertStringContainsString(
                sprintf('The table "languages" was not found in the bucket "%s" in the project ', $devBranchBucketId),
                $e->getMessage()
            );
        }
    }

    private function tableExportData(): array
    {
        $filesBasePath = __DIR__ . '/../../_data/';
        return [
            new CsvFile($filesBasePath . '1200.csv'),
            '1200.csv',
        ];
    }
}
