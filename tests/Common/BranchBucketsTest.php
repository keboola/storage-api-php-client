<?php

namespace Keboola\Test\Common;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\ClientException;
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

        try {
            foreach ([$devBucketId1, $devBucketId2] as $devBranchBucketId) {
                if ($this->_client->getBucket($devBranchBucketId)) {
                    $this->_client->dropBucket($devBranchBucketId);
                }
            }
        } catch (ClientException $e) {
        }

        // create test bucket
        $devBranchBucketId1 = $this->_client->createBucket($devBucketName1, self::STAGE_IN);
        $metadataClient = new Metadata($this->_client);
        $metadata = [
            'key' => 'KBC.createdBy.branch.id',
            'value' => $branch1['id']
        ];

        // add bucket metadata to make devBranch bucket
        $metadataClient->postBucketMetadata($devBranchBucketId1, 'system', [$metadata]);

        // add table to devBranch 1 bucket to test drop non empty bucket
        $importFile = __DIR__ . '/../_data/languages.csv';
        $devBranchTable1 = $this->_client->createTable(
            $devBranchBucketId1,
            'languages',
            new CsvFile($importFile)
        );

        // create test bucket2 to test, bucket will be dropped only for branch1 devBranch
        $devBranchBucketId2 = $this->_client->createBucket($devBucketName2, self::STAGE_IN);
        $metadataClient = new Metadata($this->_client);
        $metadata = [
            'key' => 'KBC.createdBy.branch.id',
            'value' => $branch2['id']
        ];

        // add bucket metadata to make devBranch bucket2
        $metadataClient->postBucketMetadata($devBranchBucketId2, 'system', [$metadata]);

        // test there is buckets for each dev branch
        $this->assertNotEmpty($this->_client->getBucket($devBranchBucketId1)['name']);
        $this->assertNotEmpty($this->_client->getTable($devBranchTable1));
        $this->assertNotEmpty($this->_client->getBucket($devBranchBucketId2));

        // test there is two test buckets for main branch
        $this->assertCount(2, $this->listTestBucketsForParallelTests());

        $devBranchClient->deleteBranch($branch1['id']);

        // bucket for another dev branch must exist
        $this->assertNotEmpty($this->_client->getBucket($devBranchBucketId2));

        // test main branch buckets exist
        $this->assertCount(2, $this->listTestBucketsForParallelTests());

        try {
            // test delete branch 1 remove bucket for this branch
            $this->_client->getBucket($devBranchBucketId1);
            $this->fail('should fail');
        } catch (ClientException $e) {
            $this->assertSame(404, $e->getCode());
        }
    }
}
