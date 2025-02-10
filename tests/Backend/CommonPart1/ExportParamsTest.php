<?php
/**
 * Created by JetBrains PhpStorm.
 * User: martinhalamicek
 * Date: 22/05/14
 * Time: 16:38
 * To change this template use File | Settings | File Templates.
 */

namespace Keboola\Test\Backend\CommonPart1;

use Keboola\StorageApi\Options\TokenAbstractOptions;
use Keboola\StorageApi\Options\TokenCreateOptions;
use Keboola\StorageApi\TableExporter;
use Keboola\Test\StorageApiTestCase;
use Keboola\StorageApi\Client;
use Keboola\Csv\CsvFile;

class ExportParamsTest extends StorageApiTestCase
{


    public function setUp(): void
    {
        parent::setUp();
        $this->initEmptyTestBucketsForParallelTests();
    }

    public function testInvalidExportFormat(): void
    {
        $importFile = __DIR__ . '/../../_data/languages.csv';
        $tableId = $this->_client->createTableAsync($this->getTestBucketId(), 'languages', new CsvFile($importFile));

        try {
            $this->_client->getTableDataPreview($tableId, [
                'format' => 'csv',
            ]);
            $this->fail('Should throw exception');
        } catch (\Keboola\StorageApi\Exception $e) {
            $this->assertEquals('storage.tables.validation.invalidFormat', $e->getStringCode());
        }
    }

    public function testTableExportParams(): void
    {
        $importFile = __DIR__ . '/../../_data/languages.csv';
        $tableId = $this->_client->createTableAsync($this->getTestBucketId(self::STAGE_IN), 'languages', new CsvFile($importFile));

        $originalFileLinesCount = (string) exec('wc -l <' . escapeshellarg($importFile));

        $data = $this->_client->getTableDataPreview($tableId);
        $this->assertEquals($originalFileLinesCount, count(Client::parseCsv($data, false)));

        $data = $this->_client->getTableDataPreview($tableId, [
            'limit' => 2,
        ]);
        $this->assertEquals(3, count(Client::parseCsv($data, false)), 'limit parameter');

        sleep(10);
        $startTime = time();
        $importCsv = new \Keboola\Csv\CsvFile($importFile);
        $this->_client->writeTableAsync($tableId, $importCsv, [
            'incremental' => true,
        ]);
        $this->_client->writeTableAsync($tableId, $importCsv, [
            'incremental' => true,
        ]);
        $data = $this->_client->getTableDataPreview($tableId);
        $this->assertEquals((3 * ($originalFileLinesCount - 1)) + 1, count(Client::parseCsv($data, false)), 'lines count after incremental load');

        $data = $this->_client->getTableDataPreview($tableId, [
            'changedSince' => sprintf('-%d second', ceil(time() - $startTime) + 5),
        ]);
        $this->assertEquals((2 * ($originalFileLinesCount - 1)) + 1, count(Client::parseCsv($data, false)), 'changedSince parameter');

        $data = $this->_client->getTableDataPreview($tableId, [
            'changedUntil' => sprintf('-%d second', ceil(time() - $startTime) + 5),
        ]);
        $this->assertEquals($originalFileLinesCount, count(Client::parseCsv($data, false)), 'changedUntil parameter');
    }

    /**
     * @param $exportOptions
     * @param $expectedResult
     * @dataProvider tableExportFiltersData
     */
    public function testTableExportFilters($exportOptions, $expectedResult): void
    {
        $importFile = __DIR__ . '/../../_data/users.csv';
        $tableId = $this->_client->createTableAsync($this->getTestBucketId(), 'users', new CsvFile($importFile));

        $data = $this->_client->getTableDataPreview($tableId, $exportOptions);
        $parsedData = Client::parseCsv($data, false);
        array_shift($parsedData); // remove header

        $this->assertArrayEqualsSorted($expectedResult, $parsedData, 0);
    }

    public function testTableExportShouldFailOnNonExistingColumn(): void
    {
        $importFile = __DIR__ . '/../../_data/users.csv';
        $tableId = $this->_client->createTableAsync($this->getTestBucketId(self::STAGE_IN), 'users', new CsvFile($importFile));

        try {
            $this->_client->getTableDataPreview($tableId, [
                'whereColumn' => 'mesto',
                'whereValues' => ['PRG'],
            ]);
            $this->fail('Should throw exception');
        } catch (\Keboola\StorageApi\ClientException $e) {
            $this->assertEquals('storage.tables.validation.columnNotExists', $e->getStringCode());
        }
    }

    public function testTableExportColumnsParam(): void
    {
        $importFile = __DIR__ . '/../../_data/languages.csv';
        $tableId = $this->_client->createTableAsync($this->getTestBucketId(), 'languages', new CsvFile($importFile));

        $data = $this->_client->getTableDataPreview($tableId, [
            'columns' => ['id'],
        ]);
        $parsed = Client::parseCsv($data, false);
        $firstRow = reset($parsed);

        $this->assertCount(1, $firstRow);
        $this->assertArrayHasKey(0, $firstRow);
        $this->assertEquals('id', $firstRow[0]);
    }

    public function testTableExportAsyncCache(): void
    {
        $importFile = __DIR__ . '/../../_data/users.csv';
        $tableId = $this->_client->createTableAsync($this->getTestBucketId(), 'users', new CsvFile($importFile));

        $results = $this->_client->exportTableAsync($tableId);
        $fileId = $results['file']['id'];
        $this->assertFalse($results['cacheHit']);

        // file have to available for search for caching (Elasticsearch refresh interval)
        $this->waitForFile($fileId);

        $results = $this->_client->exportTableAsync($tableId);
        $this->assertTrue($results['cacheHit']);
        $this->assertEquals($fileId, $results['file']['id']);

        $results = $this->_client->exportTableAsync($tableId, [
            'gzip' => true,
        ]);

        $gzippedFileId = $results['file']['id'];

        $this->waitForFile($gzippedFileId);
        $this->assertFalse($results['cacheHit']);
        $this->assertNotEquals($fileId, $gzippedFileId);
        $results = $this->_client->exportTableAsync($tableId, [
            'gzip' => true,
        ]);
        $this->assertTrue($results['cacheHit']);
        $this->assertEquals($gzippedFileId, $results['file']['id']);

        $results = $this->_client->exportTableAsync($tableId, [
            'whereColumn' => 'city',
            'whereValues' => ['PRG'],
        ]);
        $filteredByCityFileId = $results['file']['id'];
        $this->assertFalse($results['cacheHit']);
        $this->assertNotEquals($fileId, $filteredByCityFileId);

        $this->_client->writeTableAsync($tableId, new CsvFile($importFile));

        $results = $this->_client->exportTableAsync($tableId);
        $newFileId = $results['file']['id'];
        $this->assertFalse($results['cacheHit']);
        $this->assertNotEquals($fileId, $newFileId);

        $results = $this->_client->exportTableAsync($tableId, [
            'gzip' => true,
        ]);
        $this->assertFalse($results['cacheHit']);
    }

    /**
     * Test access to cached file by various tokens
     */
    public function testTableExportAsyncPermissions(): void
    {
        $importFile = __DIR__ . '/../../_data/users.csv';
        $tableId = $this->_client->createTableAsync($this->getTestBucketId(), 'users', new CsvFile($importFile));

        $results = $this->_client->exportTableAsync($tableId);
        $fileId = $results['file']['id'];
        $this->assertFalse($results['cacheHit']);
        $this->waitForFile($fileId);

        $tokenOptions = (new TokenCreateOptions())
            ->addBucketPermission($this->getTestBucketId(), TokenAbstractOptions::BUCKET_PERMISSION_READ)
        ;

        $newToken = $this->tokens->createToken($tokenOptions);
        $client = $this->getClient([
            'token' => $newToken['token'],
            'url' => STORAGE_API_URL,
        ]);

        $results = $client->exportTableAsync($tableId);
        $this->assertTrue($results['cacheHit']);
        $this->assertEquals($fileId, $results['file']['id']);

        $file = $client->getFile($results['file']['id']);
        Client::parseCsv(file_get_contents($file['url']), false);
    }


    public function testExportWithInternalTimestampColumn(): void
    {
        $this->allowTestForBackendsOnly([
            self::BACKEND_SNOWFLAKE,
        ]);
        $downloadPath = $this->getExportFilePathForTest('languages.sliced.csv');
        $filePath = __DIR__ . '/../../_data/languages.csv';
        $importFile = new CsvFile($filePath);

        $tableId = $this->_client->createTableAsync($this->getTestBucketId(), 'exportWithTimestamp', $importFile);

        $exporter = new TableExporter($this->_client);
        $exporter->exportTable($tableId, $downloadPath, [
            'includeInternalTimestamp' => true,
        ]);

        // compare data
        $this->assertFileExists($downloadPath);
        $exportFile = new CsvFile($downloadPath);
        $this->assertSame(['id','name','_timestamp'], $exportFile->getHeader());
    }
}
