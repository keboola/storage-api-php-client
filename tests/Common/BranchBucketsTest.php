<?php

namespace Keboola\Test\Common;

use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\DevBranches;
use Keboola\StorageApi\Metadata;
use Keboola\StorageApi\Options\GlobalSearchOptions;
use Keboola\Test\StorageApiTestCase;

class BranchBucketsTest extends StorageApiTestCase
{
    public function setUp(): void
    {
        parent::setUp();
        $this->initEmptyTestBucketsForParallelTests();
    }

    public function testDropAllDevBucketsWhenDropBranch(): void
    {
        $metadataKey = Metadata::BUCKET_METADATA_KEY_ID_BRANCH;
        $metadataProvider = Metadata::PROVIDER_SYSTEM;

        $devBranchClient = new DevBranches($this->_client);
        $metadata = new Metadata($this->_client);

        $description = $this->generateDescriptionForTestObject();

        $branchName1 = $this->generateBranchNameForParallelTest();
        $devBucketName1 = sprintf('Dev-Branch-Bucket-' . sha1($description));

        $branchName2 = $this->generateBranchNameForParallelTest('second');
        $devBucketName2 = sprintf('Second-Dev-Branch-Bucket-' . sha1($description));

        // cleanup
        $this->deleteBranchesByPrefix($devBranchClient, $branchName1);
        $branch1 = $devBranchClient->createBranch($branchName1);
        $devBranchBucketId1 = $this->initEmptyBucket($devBucketName1, Client::STAGE_IN, $description);

        $branch1TestMetadata = [
            [
                'key' => Metadata::BUCKET_METADATA_KEY_ID_BRANCH,
                'value' => $branch1['id'],
            ],
        ];

        $this->deleteBranchesByPrefix($devBranchClient, $branchName2);
        $branch2 = $devBranchClient->createBranch($branchName2);
        $devBranchBucketId2 = $this->initEmptyBucket($devBucketName2, Client::STAGE_IN, $description);

        // init data in non-dev bucket
        // create table and column with the same metadata to test delete dev branch don't delete table in main bucket
        $importFile = __DIR__ . '/../_data/languages.csv';

        $sourceTableId = $this->_client->createTableAsync(
            $this->getTestBucketId(),
            'languages',
            new CsvFile($importFile),
            [
                'primaryKey' => 'id',
            ],
        );

        $metadata->postColumnMetadata(
            sprintf('%s.%s', $sourceTableId, 'id'),
            $metadataProvider,
            $branch1TestMetadata,
        );

        $metadata->postTableMetadata(
            $sourceTableId,
            $metadataProvider,
            $branch1TestMetadata,
        );

        // init data in branch1 bucket
        $metadata->postBucketMetadata($devBranchBucketId1, $metadataProvider, $branch1TestMetadata);

        $devBranchTable1 = $this->_client->createTableAsync(
            $devBranchBucketId1,
            'languages',
            new CsvFile($importFile),
        );

        // init metadata for branch2 bucket
        $metadata->postBucketMetadata(
            $devBranchBucketId2,
            $metadataProvider,
            [
                [
                    'key' => $metadataKey,
                    'value' => $branch2['id'],
                ],
            ],
        );

        // test there is buckets for each dev branch
        $this->assertTrue($this->_client->bucketExists($devBranchBucketId1));
        $this->assertTrue($this->_client->tableExists($devBranchTable1));
        $this->assertTrue($this->_client->bucketExists($devBranchBucketId2));

        // test there is two test buckets for main branch
        $this->assertCount(2, $this->listTestBucketsForParallelTests());

        // bucket in dev branch is searchable
        $apiCall1 = fn() => $this->_client->globalSearch($devBucketName1, (new GlobalSearchOptions(null, null, null, null, ['development'])));
        $assertCallback1 = function ($searchResult) use ($devBucketName1) {
            $this->assertSame(1, $searchResult['all']);
            $this->assertArrayHasKey('id', $searchResult['items'][0]);
            $this->assertArrayHasKey('type', $searchResult['items'][0]);
            $this->assertEquals('bucket', $searchResult['items'][0]['type']);
            $this->assertArrayHasKey('name', $searchResult['items'][0]);
            $this->assertEquals($devBucketName1, $searchResult['items'][0]['name']);
        };
        $this->retryWithCallback($apiCall1, $assertCallback1);

        $devBranchClient->deleteBranch($branch1['id']);

        // bucket in dev branch is no longer searchable
        $apiCall2 = fn() => $this->_client->globalSearch($devBucketName1, (new GlobalSearchOptions(null, null, null, null, ['development'])));
        $assertCallback2 = function ($searchResult) {
            $this->assertSame(0, $searchResult['all']);
        };
        $this->retryWithCallback($apiCall2, $assertCallback2);

        // bucket for another dev branch must exist
        $this->assertNotEmpty($this->_client->getBucket($devBranchBucketId2));

        // test main branch buckets exist
        $this->assertCount(2, $this->listTestBucketsForParallelTests());

        // table and column with the same metadata should exist too
        $table = $this->_client->getTable($sourceTableId);
        $columnMetadata = reset($table['columnMetadata']['id']);

        $tableMetadata = reset($table['metadata']);
        $this->assertSame('KBC.createdBy.branch.id', $tableMetadata['key']);
        $this->assertSame('system', $tableMetadata['provider']);

        $this->assertSame('KBC.createdBy.branch.id', $columnMetadata['key']);
        $this->assertSame('system', $columnMetadata['provider']);

        // test delete branch 1 remove bucket for this branch
        $this->assertFalse($this->_client->bucketExists($devBranchBucketId1));
    }
}
