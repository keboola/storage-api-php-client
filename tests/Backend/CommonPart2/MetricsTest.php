<?php
/**
 * Created by PhpStorm.
 * User: martinhalamicek
 * Date: 21/05/15
 * Time: 13:19
 */

namespace Keboola\Test\Backend\CommonPart2;

use Keboola\Test\StorageApiTestCase;
use Keboola\StorageApi\Client;
use Keboola\Csv\CsvFile;

class MetricsTest extends StorageApiTestCase
{


    public function setUp(): void
    {
        parent::setUp();
        $this->initEmptyTestBucketsForParallelTests();
    }

    /**
     * @dataProvider importMetricsData
     * @param CsvFile $csvFile
     * @param $expectedMetrics
     */
    public function testTableCreateMetrics(CsvFile $csvFile, $expectedMetrics): void
    {
        $tokenData = $this->_client->verifyToken();
        $fileId = $this->_client->uploadFile(
            $csvFile->getPathname(),
            (new \Keboola\StorageApi\Options\FileUploadOptions())
                ->setNotify(false)
                ->setIsPublic(false)
                ->setCompress(false)
                ->setTags(['table-import'])
        );

        $bucketId = $this->getTestBucketId(self::STAGE_IN);
        $job = $this->_client->apiPost("buckets/{$bucketId}/tables-async", [
            'name' => 'languages',
            'dataFileId' => $fileId,
        ], false);

        $job = $this->_client->waitForJob($job['id']);

        if (in_array($tokenData['owner']['defaultBackend'], [
                self::BACKEND_REDSHIFT,
                self::BACKEND_SNOWFLAKE,
                self::BACKEND_SYNAPSE,
                self::BACKEND_EXASOL,
                self::BACKEND_TERADATA,
                self::BACKEND_BIGQUERY,
            ])
            && $expectedMetrics['inCompressed']) {
            $expectedMetrics['inBytesUncompressed'] = 0; // We don't know uncompressed size of file
        }

        $this->assertArrayHasKey('metrics', $job);
        $this->assertEquals($expectedMetrics, $job['metrics']);
    }

    /**
     * @dataProvider importMetricsData
     */
    public function testAsyncImportMetrics(CsvFile $csvFile, $expectedMetrics): void
    {
        $tableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN), 'languages', $csvFile);
        $tokenData = $this->_client->verifyToken();

        $fileId = $this->_client->uploadFile(
            $csvFile->getPathname(),
            (new \Keboola\StorageApi\Options\FileUploadOptions())
                ->setNotify(false)
                ->setIsPublic(false)
                ->setCompress(false)
                ->setTags(['table-import'])
        );
        $job = $this->_client->apiPost("tables/{$tableId}/import-async", [
            'dataFileId' => $fileId,
        ], false);
        $job = $this->_client->waitForJob($job['id']);

        if (in_array($tokenData['owner']['defaultBackend'], [
                self::BACKEND_REDSHIFT,
                self::BACKEND_SNOWFLAKE,
                self::BACKEND_SYNAPSE,
                self::BACKEND_EXASOL,
                self::BACKEND_TERADATA,
                self::BACKEND_BIGQUERY,
            ]) && $expectedMetrics['inCompressed']) {
            $expectedMetrics['inBytesUncompressed'] = 0; // We don't know uncompressed size of file
        }

        $this->assertArrayHasKey('metrics', $job);
        $this->assertEquals($expectedMetrics, $job['metrics']);
    }

    public function testTableExportMetrics(): void
    {
        $csvFile = new CsvFile(__DIR__ . '/../../_data/languages.csv');
        $tableId = $this->_client->createTable($this->getTestBucketId(self::STAGE_IN), 'languages', $csvFile);

        $job = $this->_client->apiPost("tables/{$tableId}/export-async", [], false);
        $job = $this->_client->waitForJob($job['id']);

        $metrics = $job['metrics'];
        $this->assertEquals(0, $metrics['inBytes']);
        $this->assertEquals(0, $metrics['inBytesUncompressed']);
        $this->assertFalse($metrics['inCompressed']);

        $this->assertFalse($metrics['outCompressed']);
        $this->assertEquals(0, $metrics['outBytes']);
        $this->assertGreaterThan(0, $metrics['outBytesUncompressed']);

        $previousMetrics = $metrics;

        // compress
        $job = $this->_client->apiPost("tables/{$tableId}/export-async", ['gzip' => true], false);
        $job = $this->_client->waitForJob($job['id']);

        $metrics = $job['metrics'];
        $this->assertEquals(0, $metrics['inBytes']);
        $this->assertEquals(0, $metrics['inBytesUncompressed']);
        $this->assertFalse($metrics['inCompressed']);

        $this->assertTrue($metrics['outCompressed']);
        $this->assertGreaterThan(0, $metrics['outBytes']);
        $this->assertEmpty($metrics['outBytesUncompressed']);
        $this->assertLessThan($metrics['outBytes'], $previousMetrics['outBytes']);
    }

    public function importMetricsData()
    {
        $csvFile = new CsvFile(__DIR__ . '/../../_data/languages.csv');
        $csvFileSize = filesize($csvFile);

        $csvFileGz = new CsvFile(__DIR__ . '/../../_data/languages.csv.gz');
        $csvFileGzSize = filesize($csvFileGz);

        return [
            [$csvFile, [
                'inCompressed' => false,
                'inBytes' => $csvFileSize,
                'inBytesUncompressed' => $csvFileSize,
                'outCompressed' => false,
                'outBytes' => 0,
                'outBytesUncompressed' => 0,
            ]],
            [$csvFileGz, [
                'inCompressed' => true,
                'inBytes' => $csvFileGzSize,
                'inBytesUncompressed' => $csvFileSize,
                'outCompressed' => false,
                'outBytes' => 0,
                'outBytesUncompressed' => 0,
            ]],
        ];
    }
}
