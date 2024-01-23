<?php

namespace Keboola\Test\Backend\CommonPart1;

use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\DevBranches;
use Keboola\Test\StorageApiTestCase;

class BranchStorageTest extends StorageApiTestCase
{
    private DevBranches $branches;

    public const SUPPORTED_BACKENDS = [
        self::BACKEND_SNOWFLAKE,
        self::BACKEND_BIGQUERY,
    ];

    public function setUp(): void
    {
        parent::setUp();
        $this->branches = new DevBranches($this->_client);
        foreach ($this->getBranchesForCurrentTestCase($this->branches) as $branch) {
            $this->branches->deleteBranch($branch['id']);
        }
    }

    public function testCreateBranchResponse(): void
    {
        $branchName = $this->generateDescriptionForTestObject();
        $job = $this->_client->apiPostJson('dev-branches/', ['name' => $branchName, 'description' => ''], false);
        do {
            $jobDone = $this->_client->getJob($job['id']);
            sleep(1);
        } while (!in_array($jobDone['status'], ['success', 'error']));

        $this->assertIsInt($job['id']);

        $this->assertStringEndsWith('/v2/storage/jobs/' . $job['id'], $job['url']);

        $this->assertMatchesRegularExpression('~\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\+\d{4}~', $job['createdTime']);

        $this->assertIsString($job['runId']);
        $this->assertMatchesRegularExpression('~\d+~', $job['runId']);

        $this->assertIsString($job['creatorToken']['id']);
        $this->assertMatchesRegularExpression('~\d+~', $job['creatorToken']['id']);

        $this->assertIsString($job['creatorToken']['description']);

        unset(
            $job['id'],
            $job['url'],
            $job['createdTime'],
            $job['runId'],
            $job['creatorToken']['id'],
            $job['creatorToken']['description'],
        );
        $this->assertSame(
            [
                'status' => 'waiting',
                'tableId' => null,
                'operationName' => 'devBranchCreate',
                'operationParams' => [
                    'queue' => 'main',
                    'values' => [
                        'name' => 'Keboola\\Test\\Backend\\CommonPart1\\BranchStorageTest\\testCreateBranchResponse',
                        'description' => '',
                    ],
                ],
                'startTime' => null,
                'endTime' => null,
                'results' => null,
                'creatorToken' => [
                ],
                'metrics' => [
                    'inCompressed' => false,
                    'inBytes' => 0,
                    'inBytesUncompressed' => 0,
                    'outCompressed' => false,
                    'outBytes' => 0,
                    'outBytesUncompressed' => 0,
                ],
            ],
            $job,
        );

        $this->assertIsInt($jobDone['id']);

        $this->assertStringEndsWith('/v2/storage/jobs/' . $jobDone['id'], $jobDone['url']);

        $this->assertMatchesRegularExpression('~\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\+\d{4}~', $jobDone['createdTime']);
        $this->assertMatchesRegularExpression('~\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\+\d{4}~', $jobDone['startTime']);
        $this->assertMatchesRegularExpression('~\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\+\d{4}~', $jobDone['endTime']);

        $this->assertIsString($jobDone['runId']);
        $this->assertMatchesRegularExpression('~\d+~', $jobDone['runId']);

        $this->assertIsString($jobDone['creatorToken']['id']);
        $this->assertMatchesRegularExpression('~\d+~', $jobDone['creatorToken']['id']);

        $this->assertIsString($jobDone['creatorToken']['description']);

        $result = $jobDone['results'];
        $this->assertIsInt($result['id']);
        $this->assertMatchesRegularExpression('~\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\+\d{4}~', $result['created']);
        $this->assertIsInt($result['creatorToken']['id']);
        $this->assertIsString($result['creatorToken']['name']);

        unset(
            $jobDone['id'],
            $jobDone['url'],
            $jobDone['createdTime'],
            $jobDone['startTime'],
            $jobDone['endTime'],
            $jobDone['runId'],
            $jobDone['creatorToken']['id'],
            $jobDone['creatorToken']['description'],
            $jobDone['results']['id'],
            $jobDone['results']['created'],
            $jobDone['results']['creatorToken']['id'],
            $jobDone['results']['creatorToken']['name'],
        );
        $this->assertSame(
            [
                'status' => 'success',
                'tableId' => null,
                'operationName' => 'devBranchCreate',
                'operationParams' => [
                    'queue' => 'main',
                    'values' => [
                        'name' => 'Keboola\\Test\\Backend\\CommonPart1\\BranchStorageTest\\testCreateBranchResponse',
                        'description' => '',
                    ],
                ],
                'results' => [
                    'name' => 'Keboola\\Test\\Backend\\CommonPart1\\BranchStorageTest\\testCreateBranchResponse',
                    'description' => '',
                    'isDefault' => false,
                    'creatorToken' => [
                    ],
                ],
                'creatorToken' => [
                ],
                'metrics' => [
                    'inCompressed' => false,
                    'inBytes' => 0,
                    'inBytesUncompressed' => 0,
                    'outCompressed' => false,
                    'outBytes' => 0,
                    'outBytesUncompressed' => 0,
                ],
            ],
            $jobDone,
        );

        $br = $this->branches->createBranch($branchName . '2');

        $this->assertIsInt($br['id']);
        $this->assertMatchesRegularExpression('~\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\+\d{4}~', $br['created']);
        $this->assertIsInt($br['creatorToken']['id']);
        $this->assertIsString($br['creatorToken']['name']);

        unset(
            $br['id'],
            $br['created'],
            $br['creatorToken']['id'],
            $br['creatorToken']['name'],
        );

        $this->assertSame(
            [
                'name' => 'Keboola\Test\Backend\CommonPart1\BranchStorageTest\testCreateBranchResponse2',
                'description' => '',
                'isDefault' => false,
                'creatorToken' => [
                ],
            ],
            $br,
        );
    }

    public function testCanCreateBucket(): void
    {
        $description = $this->generateDescriptionForTestObject();
        $bucketId = self::STAGE_IN . 'c-' . $this->getTestBucketName($description);
        try {
            $this->_client->getBucket($bucketId);
            // @phpstan-ignore: tomasfejfar-phpstan-phpunit.missingFailInTryCatch
            $this->_client->dropBucket($bucketId, ['force' => true]);
        } catch (ClientException $e) {
            // ignore if bucket not exists
        }
        $branch = $this->branches->createBranch($description);

        $backend = $this->_client->verifyToken()['owner']['defaultBackend'];
        if (!in_array(
            $backend,
            self::SUPPORTED_BACKENDS,
            true,
        )) {
            $this->expectException(ClientException::class);
            $this->expectExceptionMessage(sprintf(
                'Backend "%s" is not supported for development branch. Supported backends: "snowflake,bigquery".',
                $backend,
            ));
        } else {
            $this->expectNotToPerformAssertions();
        }
        $this->_client->getBranchAwareClient($branch['id'])
            ->createBucket(
                $this->getTestBucketName($description),
                self::STAGE_IN,
                $description,
            );
    }
}
