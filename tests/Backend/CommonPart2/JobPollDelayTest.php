<?php

namespace Keboola\Test\Backend\CommonPart2;


use Keboola\Csv\CsvFile;
use Keboola\Test\StorageApiTestCase;

class JobPollDelayTest extends StorageApiTestCase {

    public function setUp()
    {
        parent::setUp();
        $this->_initEmptyTestBuckets();
    }

    public function testRetryJobPollDelays()
    {
        $client = new \Keboola\StorageApi\Client(array(
            'token' => STORAGE_API_TOKEN,
            'url' => STORAGE_API_URL,
            'maxJobPollWaitPeriodSeconds' => 20,
        ));

        $csvFile = new CsvFile(__DIR__ . '/../../_data/languages.csv');
        $tableId = $client->createTable($this->getTestBucketId(), 'languages', $csvFile);

        $fileId = $client->uploadFile(
            $csvFile->getPathname(),
            (new \Keboola\StorageApi\Options\FileUploadOptions())
                ->setNotify(false)
                ->setIsPublic(false)
                ->setCompress(false)
                ->setTags(array('table-import'))
        );
        $start = microtime(true);
        $job = $client->apiPost("storage/tables/{$tableId}/import-async", [
            'dataFileId' => $fileId
        ], false);
        $job = $client->waitForJob($job['id']);
        $duration = microtime(true) - $start;
        // this standard job always takes 3 tries, so minimum sleep is 2 + 4 + 8 = 14 sec
        $this->assertGreaterThan(14, $duration);
    }

    public function testLinearJobPollDelay()
    {
        $client = new \Keboola\StorageApi\Client(array(
            'token' => STORAGE_API_TOKEN,
            'url' => STORAGE_API_URL,
            'retryDelay' => self::getLinearRetryMethod(),
            'maxJobPollWaitPeriodSeconds' => 20,
        ));

        $csvFile = new CsvFile(__DIR__ . '/../../_data/languages.csv');
        $tableId = $client->createTable($this->getTestBucketId(), 'languages', $csvFile);

        $fileId = $client->uploadFile(
            $csvFile->getPathname(),
            (new \Keboola\StorageApi\Options\FileUploadOptions())
                ->setNotify(false)
                ->setIsPublic(false)
                ->setCompress(false)
                ->setTags(array('table-import'))
        );
        $start = microtime(true);
        $job = $client->apiPost("storage/tables/{$tableId}/import-async", [
            'dataFileId' => $fileId
        ], false);
        $job = $client->waitForJob($job['id']);
        $duration = microtime(true) - $start;
        // this standard job consistently takes 4-5 linear tries, so minimum sleep is 1 + 2 + 3 + 4 (+ 5)  = 10 - 15 s
        $this->assertGreaterThan(10, $duration);
        // it should always finish before 17
        $this->assertLessThan(17, $duration);
    }

    public function testConstantJobPollDelay()
    {
        $client = new \Keboola\StorageApi\Client(array(
            'token' => STORAGE_API_TOKEN,
            'url' => STORAGE_API_URL,
            'retryDelay' => self::getConstantRetryMethod(),
            'maxJobPollWaitPeriodSeconds' => 20,
        ));

        $csvFile = new CsvFile(__DIR__ . '/../../_data/languages.csv');
        $tableId = $client->createTable($this->getTestBucketId(), 'languages', $csvFile);

        $fileId = $client->uploadFile(
            $csvFile->getPathname(),
            (new \Keboola\StorageApi\Options\FileUploadOptions())
                ->setNotify(false)
                ->setIsPublic(false)
                ->setCompress(false)
                ->setTags(array('table-import'))
        );
        $start = microtime(true);
        $job = $client->apiPost("storage/tables/{$tableId}/import-async", [
            'dataFileId' => $fileId
        ], false);
        $job = $client->waitForJob($job['id']);
        $duration = microtime(true) - $start;

        // this standard job consistently takes 6 - 7 constant tries, so minimum sleep is 6 - 7 seconds
        $this->assertGreaterThan(6, $duration);
        // it should always finish before 10
        $this->assertLessThan(9, $duration);
    }

    public static function getLinearRetryMethod()
    {
        return function ($tries) {
            return (int) $tries;
        };
    }

    public static function getConstantRetryMethod()
    {
        return function ($tries) {
            return 1;
        };
    }
}
