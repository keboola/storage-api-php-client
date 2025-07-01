<?php

declare(strict_types=1);

namespace Keboola\Test\Backend\CommonPart1;

use DateTime;
use DateTimeZone;
use Keboola\StorageApi\ClientException;
use Keboola\Test\StorageApiTestCase;

class ScheduledTasksTest extends StorageApiTestCase
{
    private const NON_EXISTING_BUCKET_ID = 'in.c-API-tests-nevim-dal';

    public function setUp(): void
    {
        parent::setUp();

        // Maybe?
//        $defaultBranchId = $this->getDefaultBranchId($this);
//        $this->_client = $this->getBranchAwareDefaultClient($defaultBranchId);

        $this->initEmptyTestBucketsForParallelTests(
            // client: $this->_client
        );
    }

    public function testBucketRefresh(): void
    {
        $bucketId = $this->getTestBucketId();
        $started = new DateTime();

        // Schedule task for non-existing bucket
        $exception = null;
        try {
            $this->_client->scheduleBucketRefresh(self::NON_EXISTING_BUCKET_ID, '* * * * *');
        } catch (ClientException $e) {
            $exception = $e;
        }

        $this->assertInstanceOf(ClientException::class, $exception);
        $this->assertSame(
            sprintf(
                'The bucket "%s" was not found in the project "%d"',
                self::NON_EXISTING_BUCKET_ID,
                $this->getProjectId($this->_client),
            ),
            $exception->getMessage(),
        );
        $this->assertSame(404, $exception->getCode());

        // Schedule task with invalid cron expression
        $exception = null;
        try {
            $this->_client->scheduleBucketRefresh($bucketId, 'každou středu v 13:42');
            $this->fail('Executing query with invalid cron expression should fail.');
        } catch (ClientException $e) {
            $exception = $e;
        }

        $this->assertInstanceOf(ClientException::class, $exception);
        $this->assertSame(
            "Invalid request:\n - cronExpression.cronExpression: \"Invalid format\"",
            $exception->getMessage(),
        );
        $this->assertSame(400, $exception->getCode());

        // Schedule task
        $task1 = $this->_client->scheduleBucketRefresh($bucketId, '42 13 * * 3');

        $this->assertSame(
            ['uuid', 'type', 'relatedEntity', 'relatedId', 'cronExpression', 'parameters', 'createdAt'],
            array_keys($task1),
        );

        $this->assertSame($task1['type'], 'bucket_refresh');
        $this->assertSame($task1['relatedEntity'], 'bucket');
        $this->assertSame($task1['relatedId'], $bucketId);
        $this->assertSame($task1['cronExpression'], '42 13 * * 3');
        $this->assertSame($task1['parameters'], []);

        $createdAt = DateTime::createFromFormat(\DateTimeInterface::RFC3339, $task1['createdAt']);
        $this->assertTrue($createdAt > $started);

        // Schedule another task
        $task2 = $this->_client->scheduleBucketRefresh($bucketId, '0 20 24 12 *');

        // Bucket with scheduled tasks
        $bucketWithTasks = $this->_client->getBucket($bucketId);

        $this->assertArrayHasKey('scheduledTasks', $bucketWithTasks);

        $this->assertCount(2, $bucketWithTasks['scheduledTasks']);

        $this->assertSame(
            ['uuid', 'type', 'relatedEntity', 'relatedId', 'cronExpression', 'parameters', 'createdAt'],
            array_keys($bucketWithTasks['scheduledTasks'][0]),
        );

        // Delete tasks
        $this->_client->deleteScheduledTask($task1['uuid']);
        $this->_client->deleteScheduledTask($task2['uuid']);

        // Tasks were successfully deleted
        $bucketWithoutTasks = $this->_client->getBucket($bucketId);

        $this->assertArrayHasKey('scheduledTasks', $bucketWithoutTasks);

        $this->assertEmpty($bucketWithoutTasks['scheduledTasks']);
    }
}
