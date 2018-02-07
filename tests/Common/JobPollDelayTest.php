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
        $tableId = $this->_client->createTableAsync(
            $this->getTestBucketId(),
            'languages',
            $csvFile
        );

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
        $tableId = $client->createTableAsync($this->getTestBucketId(), 'languages', $csvFile);

        $this->assertTrue($methodUsed);
    }
}
