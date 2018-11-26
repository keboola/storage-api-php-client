<?php

declare(strict_types=1);

namespace Keboola\Test\Common;

use Keboola\Csv\CsvFile;
use Keboola\Test\StorageApiTestCase;

class JobsListTest extends StorageApiTestCase
{
    public function setUp()
    {
        parent::setUp();
        $this->_initEmptyTestBuckets();
        // make 2 jobs
        for ($i = 0; $i < 2; $i++) {
            $this->_client->createTableAsync(
                $this->getTestBucketId(),
                'dummy_job_'. $i,
                new CsvFile(__DIR__ . '/../_data/languages.csv')
            );
        }
    }

    public function testJobsListing(): void
    {
        $jobs = $this->_client->listJobs();

        // the default limit is 20 jobs
        $this->assertLessThanOrEqual(20, count($jobs));

        // get only 2 jobs
        $twoJobs = $this->_client->listJobs(["limit" => 2]);
        $this->assertEquals(2, count($twoJobs));

        // check the offset parameter
        $firstJob = $twoJobs[0];
        $secondJob = $twoJobs[1];
        $offsetJobs = $this->_client->listJobs(["limit" => 2, "offset" => 1]);
        $this->assertNotEquals($firstJob, $offsetJobs[0]);
        $this->assertEquals($secondJob, $offsetJobs[0]);
    }
}
