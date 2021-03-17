<?php

namespace Keboola\Test\Common;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\DevBranches;
use Keboola\StorageApi\Metadata;
use Keboola\Test\StorageApiTestCase;

class BranchBucketsTest extends StorageApiTestCase
{
    public function setUp()
    {
        parent::setUp();
        $this->initEmptyTestBucketsForParallelTests();
    }

    public function testDropAllDevBucketsWhenDropBranch()
    {
        $devBranchClient = new DevBranches($this->_client);
        $metadata = new Metadata($this->_client);

        $branchName1 = $this->generateBranchNameForParallelTest();
        $branchName2 = $this->generateBranchNameForParallelTest('second');

        // cleanup
        $this->deleteBranchesByPrefix($devBranchClient, $branchName1);
        $this->deleteBranchesByPrefix($devBranchClient, $branchName2);

        $branch1 = $devBranchClient->createBranch($branchName1);
        $branch2 = $devBranchClient->createBranch($branchName2);

        $description = get_class($this) . '\\' . $this->getName();
        $devBucketName1 = sprintf('Dev-Branch-Bucket-' . sha1($description));
        $devBucketId1 = 'in.c-' . $devBucketName1;

        $devBucketName2 = sprintf('Second-Dev-Branch-Bucket-' . sha1($description));
        $devBucketId2 = 'in.c-' . $devBucketName2;

        foreach ([$devBucketId1, $devBucketId2] as $devBranchBucketId) {
            if ($this->_client->bucketExists($devBranchBucketId)) {
                $this->_client->dropBucket($devBranchBucketId);
            }
        }

        $importFile = __DIR__ . '/../_data/languages.csv';

        $sourceTableId = $this->_client->createTable(
            $this->getTestBucketId(),
            'languages',
            new CsvFile($importFile),
            array(
                'primaryKey' => 'id'
            )
        );

        $metadataKey = Metadata::BUCKET_METADATA_KEY_ID_BRANCH;
        $metadataProvider = Metadata::PROVIDER_SYSTEM;

        // create column and table with the same metadata to test delete dev branch don't delete table in main bucket
        $metadata->postColumnMetadata(
            sprintf('%s.%s', $sourceTableId, 'id'),
            $metadataProvider,
            [
                [
                    'key' => $metadataKey,
                    'value' => $branch1['id'],
                ],
            ]
        );

        $metadata->postTableMetadata(
            $sourceTableId,
            $metadataProvider,
            [
                [
                    'key' => $metadataKey,
                    'value' => $branch1['id'],
                ],
            ]
        );

        // create test bucket
        $devBranchBucketId1 = $this->_client->createBucket($devBucketName1, self::STAGE_IN);
        $testMetadata = [
            'key' => $metadataKey,
            'value' => $branch1['id']
        ];

        // add bucket metadata to make devBranch bucket
        $metadata->postBucketMetadata($devBranchBucketId1, $metadataProvider, [$testMetadata]);

        // add table to devBranch 1 bucket to test drop non empty bucket

        $devBranchTable1 = $this->_client->createTable(
            $devBranchBucketId1,
            'languages',
            new CsvFile($importFile)
        );

        // create test bucket2 to test, bucket will be dropped only for branch1 devBranch
        $devBranchBucketId2 = $this->_client->createBucket($devBucketName2, self::STAGE_IN);
        $testMetadata = [
            'key' => $metadataKey,
            'value' => $branch2['id']
        ];

        // add bucket metadata to make devBranch bucket2
        $metadata->postBucketMetadata($devBranchBucketId2, $metadataProvider, [$testMetadata]);

        // test there is buckets for each dev branch
        $this->assertTrue($this->_client->bucketExists($devBranchBucketId1));
        $this->assertTrue($this->_client->tableExists($devBranchTable1));
        $this->assertTrue($this->_client->bucketExists($devBranchBucketId2));

        // test there is two test buckets for main branch
        $this->assertCount(2, $this->listTestBucketsForParallelTests());

        $devBranchClient->deleteBranch($branch1['id']);

        // bucket for another dev branch must exist
        $this->assertNotEmpty($this->_client->getBucket($devBranchBucketId2));

        // test main branch buckets exist
        $this->assertCount(2, $this->listTestBucketsForParallelTests());

        // table and column with the same metadata should exist too
        $table = $this->_client->getTable($sourceTableId);
        $columnMetadata = reset($table['columnMetadata']['id']);

        $testMetadata = reset($table['metadata']);
        $this->assertSame('KBC.createdBy.branch.id', $testMetadata['key']);
        $this->assertSame('system', $testMetadata['provider']);

        $this->assertSame('KBC.createdBy.branch.id', $columnMetadata['key']);
        $this->assertSame('system', $columnMetadata['provider']);

        // test delete branch 1 remove bucket for this branch
        $this->assertFalse($this->_client->bucketExists($devBranchBucketId1));
    }
}
