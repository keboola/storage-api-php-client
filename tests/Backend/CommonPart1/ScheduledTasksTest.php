<?php

declare(strict_types=1);

namespace Keboola\Test\Backend\CommonPart1;

use DateTime;
use DateTimeInterface;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Workspaces;
use Keboola\Test\StorageApiTestCase;

class ScheduledTasksTest extends StorageApiTestCase
{
    private const NON_EXISTING_BUCKET_ID = 'in.c-API-tests-nevim-dal';
    private const EXISTING_BUCKET_NAME = 'test-successful-schedule';

    public function setUp(): void
    {
        parent::setUp();

        $this->allowTestForBackendsOnly(
            [self::BACKEND_SNOWFLAKE],
            'Test implemented only for Snowflake backend.',
        );

        $this->initEmptyTestBucketsForParallelTests();
    }

    public function testFailWithNonExternalBucket(): void
    {
        $bucketId = $this->getTestBucketId();

        $this->expectException(ClientException::class);
        $this->expectExceptionCode(400);
        $this->expectExceptionMessage('Bucket refresh is possible for external buckets only.');

        $this->_client->scheduleBucketRefresh($bucketId, '0 16 2 12 0');
    }

    public function testSuccessfulSchedule(): void
    {
        $this->dropBucketIfExists(
            $this->_client,
            sprintf('%s.%s', self::STAGE_IN, self::EXISTING_BUCKET_NAME),
            true,
        );

        $workspaces = new Workspaces($this->_client);
        $workspace = $workspaces->createWorkspace();

        $bucketId = $this->_client->registerBucket(
            self::EXISTING_BUCKET_NAME,
            [$workspace['connection']['database'], $workspace['connection']['schema']],
            self::STAGE_IN,
            'Workspace bucket registered as external',
            self::BACKEND_SNOWFLAKE,
            self::EXISTING_BUCKET_NAME,
        );

        $started = new DateTime();

        // Schedule task for non-existing bucket
        $exception = null;
        try {
            $this->_client->scheduleBucketRefresh(self::NON_EXISTING_BUCKET_ID, '* * * * *');
            $this->fail('Scheduling bucket with invalid cron expression should fail.');
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

        // Schedule a task with invalid cron expression
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
            ['uuid', 'job', 'relatedEntity', 'relatedId', 'cronExpression', 'createdAt'],
            array_keys($task1),
        );

        $this->assertSame($task1['job'], 'bucketRefresh');
        $this->assertSame($task1['relatedEntity'], 'bucket');
        $this->assertSame($task1['relatedId'], $bucketId);
        $this->assertSame($task1['cronExpression'], '42 13 * * 3');

        $createdAt = DateTime::createFromFormat(DateTimeInterface::RFC3339, $task1['createdAt']);
        $this->assertTrue($createdAt > $started);

        // Schedule another task
        $task2 = $this->_client->scheduleBucketRefresh($bucketId, '0 20 24 12 *');

        // Bucket with scheduled tasks
        $bucketWithTasks = $this->_client->getBucket($bucketId);

        $this->assertArrayHasKey('scheduledTasks', $bucketWithTasks);

        $this->assertCount(2, $bucketWithTasks['scheduledTasks']);

        $this->assertSame(
            ['uuid', 'job', 'relatedEntity', 'relatedId', 'cronExpression', 'createdAt'],
            array_keys($bucketWithTasks['scheduledTasks'][0]),
        );

        // Delete tasks
        $this->_client->deleteScheduledTask($task1['uuid']);

        // Tasks were successfully deleted
        $bucketWithoutTasks = $this->_client->getBucket($bucketId);

        $this->assertArrayHasKey('scheduledTasks', $bucketWithoutTasks);
        $this->assertCount(1, $bucketWithoutTasks['scheduledTasks']);

        // Drop bucket
        // Related scheduled tasks are deleted in cascade, but we're not able to verify it).
        $this->_client->dropBucket($bucketId);
    }
}
