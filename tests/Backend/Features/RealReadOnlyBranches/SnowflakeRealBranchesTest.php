<?php

declare(strict_types=1);

namespace Keboola\Test\Backend\Features\RealReadOnlyBranches;

use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\DevBranches;
use Keboola\StorageApi\Workspaces;
use Keboola\Test\Backend\Workspaces\ParallelWorkspacesTestCase;
use Keboola\Test\Backend\Workspaces\Backend\WorkspaceBackendFactory;

class SnowflakeRealBranchesTest extends ParallelWorkspacesTestCase
{
    private DevBranches $branches;

    public function setUp(): void
    {
        parent::setUp();
        $this->branches = new DevBranches($this->_client);
        $this->cleanupTestBranches($this->_client);
    }

    /**
     * This test verifies visibility of buckets from workspaces across default and dev branches
     * when project has storage-branches feature set only.
     * Scenario:
     *  1. Create bucket in default branch.
     *  2. Create dev branch and create bucket inside it.
     *  3. Assert backendPath for both buckets.
     *  4. Create workspace in default branch – ensure dev bucket NOT visible.
     *  5. Create workspace in dev branch – ensure BOTH buckets visible.
     *  6. Drop dev branch – ensure its bucket and workspace are deleted (no longer accessible).
     */
    public function testBucketsVisibilityBetweenBranchWorkspaces(): void
    {
        $this->allowTestForBackendsOnly([
            self::BACKEND_SNOWFLAKE,
        ]);
        $tokenInfo = $this->_client->verifyToken();
        if (!in_array('storage-branches', $tokenInfo['owner']['features'], true)) {
            $this->markTestSkipped('Project does not have storage-branches feature.');
        }

        // Default branch bucket already created in setUp() via initEmptyTestBucketsForParallelTests
        $defaultBucketId = $this->getTestBucketId(self::STAGE_IN);
        $defaultInBucket = $this->_client->getBucket($defaultBucketId);
        $defaultOutBucket = $this->_client->getBucket($this->getTestBucketId(self::STAGE_OUT));
        // create table in bucket
        $csv = $this->createTempCsv();
        $csv->writeRow(['Name', 'Id']);
        $csv->writeRow(['aabb', 'test']);
        $this->_client->createTableAsync(
            $defaultInBucket['id'],
            'test1',
            $csv,
        );

        // Prepare unique suffix for dev resources
        $devBucketName = $this->getTestBucketName($this->generateDescriptionForTestObject());

        // Create dev branch and its bucket
        $branch = $this->branches->createBranch($this->generateDescriptionForTestObject());
        $branchId = $branch['id'];

        $devClient = $this->_client->getBranchAwareClient($branchId);

        $devBucketId = $this->initEmptyBucket($devBucketName, self::STAGE_IN, $this->generateDescriptionForTestObject(), $devClient);
        $devBucket = $devClient->getBucket($devBucketId);
        $devClient->createTableAsync(
            $devBucket['id'],
            'test1',
            $csv,
        );

        // Assert backendPath for both buckets
        $this->assertStringStartsWith('in.c-', $defaultInBucket['backendPath'][1]);
        $this->assertStringStartsWith($branch['id'] . '_in.c-', $devBucket['backendPath'][1]);

        // Workspace in default branch – should NOT see dev bucket
        $wsDefault = $this->initTestWorkspace(
            self::BACKEND_SNOWFLAKE,
            [],
            true,
            true,
            $this->_client,
        );
        $wsBackend = WorkspaceBackendFactory::createWorkspaceBackend($wsDefault, true);
        // get list of schemas visible to workspace user
        /**
         * @var array<array{name:string}> $schemas
         */
        //@phpstan-ignore-next-line
        $schemas = $wsBackend->getDb()->fetchAllAssociative('SHOW SCHEMAS WITH PRIVILEGES USAGE;');
        $this->assertExpectedContainsSchemaNames([
            $wsDefault['connection']['schema'],
            $defaultInBucket['backendPath'][1],
            $defaultOutBucket['backendPath'][1],
        ], $schemas);
        $this->assertExpectedNotContainsSchemaNames([
            $devBucket['backendPath'][1],
        ], $schemas);

        // Workspace in dev branch – should see both buckets (create via helpers with dev client)
        $wsDev = $this->initTestWorkspace(
            self::BACKEND_SNOWFLAKE,
            [],
            true,
            true,
            $devClient,
        );
        $wsDevBackend = WorkspaceBackendFactory::createWorkspaceBackend($wsDev, true);
        /**
         * @var array<array{name:string}> $schemas
         */
        //@phpstan-ignore-next-line
        $schemas = $wsDevBackend->getDb()->fetchAllAssociative('SHOW SCHEMAS WITH PRIVILEGES USAGE;');
        // get list of schemas visible to workspace user in dv branch
        $this->assertExpectedContainsSchemaNames([
            $devBucket['backendPath'][1],
            $wsDev['connection']['schema'], // user can see dev branch schema
            $defaultInBucket['backendPath'][1],
            $defaultOutBucket['backendPath'][1],
        ], $schemas);

        // Drop dev branch – dev bucket + dev workspace should be gone
        $this->branches->deleteBranch($branchId);

        // Default bucket must still exist
        $remainingDefaultBucket = $this->_client->getBucket($defaultBucketId);
        $this->assertSame($defaultBucketId, $remainingDefaultBucket['id']);

        try {
            (new Workspaces($this->workspaceSapiClient->getBranchAwareClient($branchId)))
                ->getWorkspace($wsDev['id']);
            $this->fail('Dev workspace still exists after branch deletion');
        } catch (ClientException $e) {
            $this->assertSame(404, $e->getCode());
        }

        try {
            $devClient->getBucket($devBucketId);
            $this->fail('Dev bucket still exists after branch deletion');
        } catch (ClientException $e) {
            $this->assertSame(404, $e->getCode());
        }
    }

    /**
     * @param string[] $expected
     * @param array<array{name:string}> $schemas
     */
    private function assertExpectedContainsSchemaNames(array $expected, array $schemas): void
    {
        $schemaNames = array_map(fn($r) => $r['name'], $schemas);
        foreach ($expected as $expectedSchema) {
            $this->assertContains(
                $expectedSchema,
                $schemaNames,
                sprintf('Schema "%s" not found in "%s"', $expectedSchema, implode(', ', $schemaNames)),
            );
        }
    }

    /**
     * @param string[] $notExpected
     * @param array<array{name:string}> $schemas
 */
    private function assertExpectedNotContainsSchemaNames(array $notExpected, array $schemas): void
    {
        $schemaNames = array_map(fn($r) => $r['name'], $schemas);
        foreach ($notExpected as $expectedSchema) {
            $this->assertNotContains(
                $expectedSchema,
                $schemaNames,
                sprintf('Schema "%s" was found in "%s" that was not expected.', $expectedSchema, implode(', ', $schemaNames)),
            );
        }
    }
}
