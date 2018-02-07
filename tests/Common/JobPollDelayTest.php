<?php

namespace Keboola\Test\Common;

use Keboola\Csv\CsvFile;
use Keboola\Test\StorageApiTestCase;

class JobPollDelayTest extends StorageApiTestCase
{

    public function setUp()
    {
        parent::setUp();
        $this->_initEmptyTestBuckets();
    }

    public function testSetJobPollDelay()
    {
        $methodUsed = false;

        $linearDelay = function ($tries) use (&$methodUsed) {
            $methodUsed = true;
            return (int) $tries;
        };

        $this->_client->setJobPollDelayMethoc($linearDelay);

        $csvFile = new CsvFile(__DIR__ . '/../_data/languages.csv');
        $tableId = $this->_client->createTable($this->getTestBucketId(), 'languages', $csvFile);

        $fileId = $this->_client->uploadFile(
            $csvFile->getPathname(),
            (new \Keboola\StorageApi\Options\FileUploadOptions())
                ->setNotify(false)
                ->setIsPublic(false)
                ->setCompress(false)
                ->setTags(array('table-import'))
        );
        $job = $this->_client->apiPost("storage/tables/{$tableId}/import-async", [
            'dataFileId' => $fileId
        ], false);
        $job = $this->_client->waitForJob($job['id']);

        $this->assertTrue($methodUsed);
    }

    public function testAlternateJobPollDelay()
    {
        $methodUsed = false;

        $linearDelay = function ($tries) use (&$methodUsed) {
            $methodUsed = true;
            return (int) $tries;
        };

        $client = new \Keboola\StorageApi\Client(array(
            'token' => STORAGE_API_TOKEN,
            'url' => STORAGE_API_URL,
            'retryDelay' => $linearDelay,
            'maxJobPollWaitPeriodSeconds' => 20,
        ));

        $csvFile = new CsvFile(__DIR__ . '/../_data/languages.csv');
        $tableId = $client->createTable($this->getTestBucketId(), 'languages', $csvFile);

        $fileId = $client->uploadFile(
            $csvFile->getPathname(),
            (new \Keboola\StorageApi\Options\FileUploadOptions())
                ->setNotify(false)
                ->setIsPublic(false)
                ->setCompress(false)
                ->setTags(array('table-import'))
        );
        $job = $client->apiPost("storage/tables/{$tableId}/import-async", [
            'dataFileId' => $fileId
        ], false);
        $job = $client->waitForJob($job['id']);

        $this->assertTrue($methodUsed);
    }
}
