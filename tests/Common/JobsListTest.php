<?php



namespace Keboola\Test\Common;

use DateTime;
use DateTimeInterface;
use Keboola\Csv\CsvFile;
use Keboola\Test\StorageApiTestCase;

class JobsListTest extends StorageApiTestCase
{
    public function setUp(): void
    {
        parent::setUp();
        $this->_initEmptyTestBuckets();
        // make 2 jobs
        for ($i = 0; $i < 2; $i++) {
            $this->_client->createTableAsync(
                $this->getTestBucketId(),
                'dummy_job_'. $i,
                new CsvFile(__DIR__ . '/../_data/languages.csv'),
            );
        }
    }

    public function testJobsListing(): void
    {
        $jobs = $this->_client->listJobs();

        // the default limit is 20 jobs
        $this->assertLessThanOrEqual(20, count($jobs));

        // get only 2 jobs
        $twoJobs = $this->_client->listJobs(['limit' => 2]);
        $this->assertEquals(2, count($twoJobs));

        $errorJobs = $this->_client->listJobs(['storage_job_status' => ['error']]);
        foreach ($errorJobs as $errorJob) {
            if ($errorJob['status'] !== 'error') {
                $this->fail(sprintf('Job have different status: %s', $errorJob['status']));
            }
        }

        $successJobs = $this->_client->listJobs(['storage_job_status' => ['success']]);
        foreach ($successJobs as $successJob) {
            if ($successJob['status'] !== 'success') {
                $this->fail(sprintf('Job have different status: %s', $successJob['status']));
            }
        }

        $now = new DateTime('now');
        $startBeforeJobs = $this->_client->listJobs(['start_time_to' => $now->format('Y-m-d H:i:s')]);
        foreach ($startBeforeJobs as $startBeforeJob) {
            if (DateTime::createFromFormat(DateTimeInterface::RFC3339, $startBeforeJob['startTime']) > $now) {
                $this->fail(sprintf('Job started after: %s', $startBeforeJob['startTime']));
            }
        }

        $fromTime = new DateTime('-1 hour');
        $toTime = new DateTime('now');
        $startBetweenJobs = $this->_client->listJobs([
            'start_time_from' => $fromTime->format('Y-m-d H:i:s'),
            'start_time_to' => $toTime->format('Y-m-d H:i:s'),
            ]);
        foreach ($startBetweenJobs as $betweenJob) {
            if (DateTime::createFromFormat(DateTimeInterface::RFC3339, $betweenJob['startTime']) > $fromTime && DateTime::createFromFormat(DateTimeInterface::RFC3339, $betweenJob['startTime']) < $toTime) {
                continue;
            }
            $this->fail(sprintf('Job started outside interval: %s', $betweenJob['startTime']));
        }

        // check the offset parameter
        $firstJob = $twoJobs[0];
        $secondJob = $twoJobs[1];
        $offsetJobs = $this->_client->listJobs(['limit' => 2, 'offset' => 1]);
        $this->assertNotEquals($firstJob, $offsetJobs[0]);
        $this->assertEquals($secondJob, $offsetJobs[0]);
    }
}
