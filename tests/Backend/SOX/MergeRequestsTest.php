<?php

namespace Keboola\Test\Backend\SOX;

use Generator;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\DevBranches;
use Keboola\StorageApi\Options\Components\Configuration;
use Keboola\StorageApi\Options\Components\ConfigurationMetadata;
use Keboola\StorageApi\Options\Components\ConfigurationRow;
use Keboola\StorageApi\Options\Components\ConfigurationRowState;
use Keboola\StorageApi\Options\Components\ConfigurationState;
use Keboola\StorageApi\Options\Components\ListComponentConfigurationsOptions;
use Keboola\StorageApi\Options\Components\ListConfigurationMetadataOptions;
use Keboola\StorageApi\Options\Components\ListConfigurationRowsOptions;
use Keboola\StorageApi\Options\Components\ListConfigurationRowVersionsOptions;
use Keboola\StorageApi\Options\Components\ListConfigurationVersionsOptions;
use Keboola\Test\StorageApiTestCase;
use Keboola\Test\Utils\EventsQueryBuilder;
use Keboola\Test\Utils\MetadataUtils;
use Throwable;

class MergeRequestsTest extends StorageApiTestCase
{
    use MetadataUtils;

    private Client $developerClient;

    private Client $prodManagerClient;

    private DevBranches $branches;

    public function setUp(): void
    {
        parent::setUp();
        $this->prodManagerClient = $this->getDefaultClient();
        $this->developerClient = $this->getDeveloperStorageApiClient();
        $this->branches = new DevBranches($this->developerClient);
        foreach ($this->getBranchesForCurrentTestCase($this->branches) as $branch) {
            try {
                // branch is being deleted in async job when MR being merged. So can skip 404 exceptions
                $this->branches->deleteBranch($branch['id']);
            } catch (Throwable $e) {
                // this could fail in both job and http request but with same message
                if ($e->getMessage() !== sprintf('Branch id:"%s" not found', $branch['id'])) {
                    throw $e;
                }
            }
        }

        $this->cleanupConfigurations($this->getDefaultBranchStorageApiClient());
    }

    public function testCreateMergeRequest(): void
    {
        $defaultBranch = $this->branches->getDefaultBranch();
        $newBranch = $this->branches->createBranch($this->generateDescriptionForTestObject() . '_aaaa');

        $mrId = $this->developerClient->createMergeRequest([
            'branchFromId' => $newBranch['id'],
            'branchIntoId' => $defaultBranch['id'],
            'title' => 'Change everything',
            'description' => 'Fix typo',
        ]);

        $mrData = $this->developerClient->getMergeRequest($mrId);

        $this->assertEquals('Change everything', $mrData['title']);
        // check that detail also containts content
        $this->assertArrayHasKey('changeLog', $mrData);
        $this->assertSame([], $mrData['changeLog'], 'Content of an empty MR should be empty object');
    }

    public function testCreateMergeRequestFromInvalidBranches(): void
    {
        $this->expectExceptionMessage('Cannot create merge request. Branch id:"345" was not found.');
        $this->developerClient->createMergeRequest([
            'branchFromId' => 123,
            'branchIntoId' => 345,
            'title' => 'Change everything',
            'description' => 'Fix typo',
        ]);
    }

    public function testCreateMergeRequestIntoDevBranch(): void
    {
        $this->expectExceptionMessage('Cannot create merge request. Target branch is not default.');

        $defaultBranch = $this->branches->getDefaultBranch();
        $newBranch = $this->branches->createBranch($this->generateDescriptionForTestObject() . '_aaaa');

        $this->developerClient->createMergeRequest([
            'branchFromId' => $defaultBranch['id'],
            'branchIntoId' => $newBranch['id'],
            'title' => 'Change everything',
            'description' => 'Fix typo',
        ]);
    }

    public function testPutInReview(): void
    {
        $defaultBranch = $this->branches->getDefaultBranch();
        $newBranch = $this->branches->createBranch($this->generateDescriptionForTestObject() . '_aaaa');

        $title = 'Change everything ' . time();
        $mrId = $this->developerClient->createMergeRequest([
            'branchFromId' => $newBranch['id'],
            'branchIntoId' => $defaultBranch['id'],
            'title' => $title,
            'description' => 'Fix typo',
        ]);

        $list = $this->developerClient->listMergeRequests();
        self::assertSame($title, $list[0]['title']);

        $mrData = $this->developerClient->mergeRequestRequestReview($mrId);

        $this->assertEquals('in_review', $mrData['state']);
    }

    public function testMRWorkflowFromDevelopmentToCancelWithEvents(): void
    {
        // init everything
        $privClient = $this->getDefaultBranchStorageApiClient();

        $defaultBranch = $this->branches->getDefaultBranch();

        $newBranch = $this->branches->createBranch($this->generateDescriptionForTestObject() . '_aaaa');
        $creatorId = $this->developerClient->verifyToken()['admin']['id'];

        // create MR
        $this->initEvents($privClient);
        $mrId = $this->developerClient->createMergeRequest([
            'branchFromId' => $newBranch['id'],
            'branchIntoId' => $defaultBranch['id'],
            'title' => 'Change everything',
            'description' => 'Fix typo',
        ]);
        $eventsQuery = new EventsQueryBuilder();
        $eventsQuery->setEvent('storage.mergeRequestStateChanged');
        $eventsQuery->setObjectId((string) $mrId);
        $eventsQuery->setObjectType('mergeRequest');

        $assertCallbackForCreated = function ($events) use ($mrId, $creatorId) {
            $this->assertCount(1, $events);
            $this->assertEquals([
                'creatorId' => $creatorId,
                'mergeRequestId' => $mrId,
            ], $events[0]['params']);
        };
        $eventsQueryForCreate = new EventsQueryBuilder();
        $eventsQueryForCreate->setEvent('storage.mergeRequestCreated');
        $eventsQueryForCreate->setObjectId((string) $mrId);
        $eventsQueryForCreate->setObjectType('mergeRequest');

        $this->assertEventWithRetries($this->getDefaultClient(), $assertCallbackForCreated, $eventsQueryForCreate);

        $this->initEvents($privClient);
        // lets go!
        $reviewerClient = $this->getReviewerStorageApiClient();

        // request review
        $this->developerClient->mergeRequestRequestReview($mrId);

        $assertCallback = function ($events) use ($mrId) {
            $this->assertCount(1, $events);
            $this->assertEquals([
                'operation' => 'request_review',
                'stateFrom' => 'development',
                'stateTo' => 'in_review',
                'mergeRequestId' => $mrId,
            ], $events[0]['params']);
        };

        $this->assertEventWithRetries($this->getDefaultClient(), $assertCallback, $eventsQuery);

        // add first approval
        $this->initEvents($privClient);
        $mrData = $reviewerClient->mergeRequestApprove($mrId);

        $assertCallback = function ($events) use ($mrId) {
            $this->assertCount(1, $events);
            $this->assertEquals([
                'operation' => 'approve',
                'stateFrom' => 'in_review',
                'stateTo' => 'in_review',
                'mergeRequestId' => $mrId,
            ], $events[0]['params']);
        };
        $this->assertEventWithRetries($this->getDefaultClient(), $assertCallback, $eventsQuery);

        $this->assertEquals('in_review', $mrData['state']);
        $this->assertCount(1, $mrData['approvals']);

        // add second approval -> all finish review
        $this->initEvents($privClient);
        $mrData = $this->getSecondReviewerStorageApiClient()->mergeRequestApprove($mrId);

        $assertCallback = function ($events) use ($mrId) {
            $this->assertCount(2, $events);
            $this->assertEquals([
                'operation' => 'finish_review',
                'stateFrom' => 'in_review',
                'stateTo' => 'approved',
                'mergeRequestId' => $mrId,
            ], $events[0]['params']);

            $this->assertEquals([
                'operation' => 'approve',
                'stateFrom' => 'in_review',
                'stateTo' => 'in_review',
                'mergeRequestId' => $mrId,
            ], $events[1]['params']);
        };
        $this->assertEventWithRetries($this->getDefaultClient(), $assertCallback, $eventsQuery);

        $this->assertEquals('approved', $mrData['state']);
        $this->assertCount(2, $mrData['approvals']);

        // request changes
        $this->initEvents($privClient);
        $mrData = $reviewerClient->requestMergeRequestChanges($mrId);

        $assertCallback = function ($events) use ($mrId) {
            $this->assertCount(1, $events);
            $this->assertEquals([
                'operation' => 'request_changes',
                'stateFrom' => 'approved',
                'stateTo' => 'development',
                'mergeRequestId' => $mrId,
            ], $events[0]['params']);
        };
        $this->assertEventWithRetries($this->getDefaultClient(), $assertCallback, $eventsQuery);

        $this->assertCount(0, $mrData['approvals']);
        $this->assertSame('development', $mrData['state']);

        // cancel MR
        $this->initEvents($privClient);
        (new DevBranches($reviewerClient))->deleteBranch($newBranch['id']);
        $assertCallback = function ($events) use ($mrId) {
            $this->assertCount(1, $events);
            $this->assertEquals([
                'operation' => 'cancel',
                'stateFrom' => 'development',
                'stateTo' => 'canceled',
                'mergeRequestId' => $mrId,
            ], $events[0]['params']);
        };
        $this->assertEventWithRetries($this->getDefaultClient(), $assertCallback, $eventsQuery);
        $this->assertBranchIsDeleted($newBranch['id']);

        $mrData = $reviewerClient->getMergeRequest($mrId);
        $this->assertCount(0, $mrData['approvals']);
        $this->assertSame('canceled', $mrData['state']);
        $this->assertNull($mrData['branches']['branchFromId']);
    }

    public function testMRWorkflowLock(): void
    {
        $defaultBranch = $this->branches->getDefaultBranch();

        // create two branches
        $newBranch1 = $this->branches->createBranch($this->generateDescriptionForTestObject() . '_1');
        $newBranch2 = $this->branches->createBranch($this->generateDescriptionForTestObject() . '_2');

        // create same configuration in branches to create conflict
        $configuration = (new Configuration())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setName('Main')
            ->setDescription('some desc');
        (new Components(
            $this->getBranchAwareClient($newBranch1['id'], [
                'token' => STORAGE_API_DEVELOPER_TOKEN,
                'url' => STORAGE_API_URL,
            ])
        ))->addConfiguration($configuration);
        (new Components(
            $this->getBranchAwareClient($newBranch2['id'], [
                'token' => STORAGE_API_DEVELOPER_TOKEN,
                'url' => STORAGE_API_URL,
            ])
        ))->addConfiguration($configuration);

        // create MR for each branch
        $mr1 = $this->developerClient->createMergeRequest([
            'branchFromId' => $newBranch1['id'],
            'branchIntoId' => $defaultBranch['id'],
            'title' => 'Change everything',
            'description' => 'Fix typo',
        ]);
        $mr2 = $this->developerClient->createMergeRequest([
            'branchFromId' => $newBranch2['id'],
            'branchIntoId' => $defaultBranch['id'],
            'title' => 'Change everything',
            'description' => 'Fix typo',
        ]);

        // set both MR to approved
        $this->developerClient->mergeRequestRequestReview($mr1);
        $this->developerClient->mergeRequestRequestReview($mr2);
        $this->getReviewerStorageApiClient()->mergeRequestApprove($mr1);
        $this->getReviewerStorageApiClient()->mergeRequestApprove($mr2);
        $this->getSecondReviewerStorageApiClient()->mergeRequestApprove($mr1);
        $this->getSecondReviewerStorageApiClient()->mergeRequestApprove($mr2);

        $this->initEvents($this->getDefaultBranchStorageApiClient());
        // merge first MR
        /** @var array{id: int} $mrJob */
        $mrJob = $this->prodManagerClient->apiPutJson("merge-request/{$mr1}/merge", [], false);
        try {
            // try to create configuration on branch in_merge
            (new Components(
                $this->getBranchAwareClient($newBranch1['id'], [
                    'token' => STORAGE_API_DEVELOPER_TOKEN,
                    'url' => STORAGE_API_URL,
                ])
            ))->addConfiguration($configuration);
        } catch (ClientException $e) {
            // error messages should be improved in the future SOX-155
            $this->assertSame(
                'You don\'t have access to the resource.',
                $e->getMessage()
            );
        }
        try {
            // try to merge second MR
            // this is not 100% reliable
            // MR1 merge job could end before this request and job for MR2 could be queued
            $this->prodManagerClient->apiPutJson("merge-request/{$mr2}/merge", [], false);
            $this->fail(sprintf(
                'Second MR cannot be merged. (This could be race-condition because "%s" job ended before this request)',
                $mrJob['id']
            ));
        } catch (ClientException $e) {
            $this->assertSame('Cannot merge, another merge is in progress.', $e->getMessage());
        }

        $this->developerClient->waitForJob($mrJob['id']);

        // MR1 is published MR2 is approved as merge had failed
        $mr = $this->developerClient->getMergeRequest($mr1);
        $this->assertEquals('published', $mr['state']);
        $mr = $this->developerClient->getMergeRequest($mr2);
        $this->assertEquals('approved', $mr['state']);

        $devBranch = new DevBranches($this->developerClient);
        $branch2 = $devBranch->getBranch($newBranch2['id']);
        $this->assertNotEmpty($branch2);

        $this->assertBranchIsDeleted($newBranch1['id']);
    }

    public function testAddSingleApprovalOnly(): void
    {
        $defaultBranch = $this->branches->getDefaultBranch();

        $newBranch = $this->branches->createBranch($this->generateDescriptionForTestObject() . '_aaaa');

        $mrId = $this->developerClient->createMergeRequest([
            'branchFromId' => $newBranch['id'],
            'branchIntoId' => $defaultBranch['id'],
            'title' => 'Change everything',
            'description' => 'Fix typo',
        ]);

        $reviewerClient = $this->getReviewerStorageApiClient();
        $this->developerClient->mergeRequestRequestReview($mrId);

        $mrData = $reviewerClient->mergeRequestApprove($mrId);

        $this->assertEquals('in_review', $mrData['state']);
        $this->assertCount(1, $mrData['approvals']);

        try {
            $mrData = $reviewerClient->mergeRequestApprove($mrId);
        } catch (ClientException $e) {
            $this->assertSame('Operation canot be performed due: This reviewer has already approved this request.', $e->getMessage());
        }
    }

    public function testProdManagerCannotPutBranchInReview(): void
    {
        $defaultBranch = $this->branches->getDefaultBranch();
        $newBranch = $this->branches->createBranch($this->generateDescriptionForTestObject() . '_aaaa');

        try {
            $this->prodManagerClient->createMergeRequest([
                'branchFromId' => $newBranch['id'],
                'branchIntoId' => $defaultBranch['id'],
                'title' => 'Change everything',
                'description' => 'Fix typo',
            ]);
            $this->fail('Prod manager should not be able to create merge request');
        } catch (ClientException $e) {
            $this->assertSame($e->getMessage(), 'You don\'t have access to the resource.');
        }

        $mrId = $this->developerClient->createMergeRequest([
            'branchFromId' => $newBranch['id'],
            'branchIntoId' => $defaultBranch['id'],
            'title' => 'Change everything',
            'description' => 'Fix typo',
        ]);

        try {
            $this->prodManagerClient->mergeRequestRequestReview($mrId);
            $this->fail('Prod manager should not be able to put merge request in review');
        } catch (ClientException $e) {
            $this->assertSame($e->getMessage(), 'You don\'t have access to the resource.');
        }
    }
    public function testProdManagerCanRequestChanges(): void
    {
        $defaultBranch = $this->branches->getDefaultBranch();
        $newBranch = $this->branches->createBranch($this->generateDescriptionForTestObject() . '_aaaa');

        // create and approve MR
        $mrId = $this->developerClient->createMergeRequest([
            'branchFromId' => $newBranch['id'],
            'branchIntoId' => $defaultBranch['id'],
            'title' => 'Change everything',
            'description' => 'Fix typo',
        ]);

        $reviewerClient = $this->getReviewerStorageApiClient();
        $this->developerClient->mergeRequestRequestReview($mrId);

        $reviewerClient->mergeRequestApprove($mrId);
        $this->getSecondReviewerStorageApiClient()->mergeRequestApprove($mrId);

        // request changes by PM and check the events
        $this->initEvents($this->getDefaultBranchStorageApiClient());
        $mrData = $this->prodManagerClient->requestMergeRequestChanges($mrId);

        $assertCallback = function ($events) use ($mrId) {
            $this->assertCount(1, $events);
            $this->assertEquals([
                'operation' => 'request_changes',
                'stateFrom' => 'approved',
                'stateTo' => 'development',
                'mergeRequestId' => $mrId,
            ], $events[0]['params']);
        };

        $eventsQuery = new EventsQueryBuilder();
        $eventsQuery->setEvent('storage.mergeRequestStateChanged');
        $eventsQuery->setObjectId((string) $mrId);
        $eventsQuery->setObjectType('mergeRequest');
        $this->assertEventWithRetries($this->getDefaultClient(), $assertCallback, $eventsQuery);

        $this->assertEquals('development', $mrData['state']);
    }

    public function testUpdateMR(): void
    {
        $defaultBranch = $this->branches->getDefaultBranch();

        $componentId = 'wr-db';
        $configurationId = 'main-1';
        $components = new Components($this->getDefaultBranchStorageApiClient());

        $configuration = (new Configuration())
            ->setComponentId($componentId)
            ->setConfigurationId($configurationId)
            ->setName('Main')
            ->setDescription('some desc');
        $components->addConfiguration($configuration);

        $newBranch = $this->branches->createBranch($this->generateDescriptionForTestObject() . '_aaaa');

        $devBranchComponents = new Components($this->getBranchAwareClient($newBranch['id'], [
            'token' => STORAGE_API_DEVELOPER_TOKEN,
            'url' => STORAGE_API_URL,
        ]));
        $devBranchComponents->addConfigurationRow((new ConfigurationRow($configuration))
            ->setRowId('firstRow')
            ->setConfiguration(['value' => 1]));

        $mrId = $this->developerClient->createMergeRequest([
            'branchFromId' => $newBranch['id'],
            'branchIntoId' => $defaultBranch['id'],
            'title' => 'Change everything',
            'description' => 'Fix typo',
        ]);

        try {
            $this->prodManagerClient->updateMergeRequest(
                $mrId,
                'Lalala',
                'Trololo',
            );
            $this->fail('Prod manager should not be able to create merge request');
        } catch (ClientException $e) {
            $this->assertSame($e->getMessage(), 'You don\'t have access to the resource.');
        }

        $this->developerClient->mergeRequestRequestReview($mrId);

        try {
            $this->developerClient->updateMergeRequest(
                $mrId,
                'Lalala',
                'Trololo',
            );
            $this->fail('MR in review should not be able to update');
        } catch (ClientException $e) {
            $this->assertSame($e->getMessage(), 'You don\'t have access to the resource.');
        }

        $this->developerClient->requestMergeRequestChanges($mrId);
        $mr = $this->developerClient->updateMergeRequest(
            $mrId,
            'Lalala',
            'Trololo',
        );

        $this->assertSame('Lalala', $mr['title']);
        $this->assertSame('Trololo', $mr['description']);

        // different user should also be able to update it
        $mr = $this->getReviewerStorageApiClient()->updateMergeRequest(
            $mrId,
            'By reviewer',
            'With love to developer',
        );

        $this->assertSame('By reviewer', $mr['title']);
        $this->assertSame('With love to developer', $mr['description']);
    }

    /** @dataProvider cantMergeTokenProviders */
    public function testSpecificRolesCantMerge(Client $client): void
    {
        $defaultBranch = $this->branches->getDefaultBranch();

        $newBranch = $this->branches->createBranch($this->generateDescriptionForTestObject() . '_aaaa');

        $mrId = $this->developerClient->createMergeRequest([
            'branchFromId' => $newBranch['id'],
            'branchIntoId' => $defaultBranch['id'],
            'title' => 'Change everything',
            'description' => 'Fix typo',
        ]);

        $reviewerClient = $this->getReviewerStorageApiClient();
        $this->developerClient->mergeRequestRequestReview($mrId);

        $mrData = $reviewerClient->mergeRequestApprove($mrId);

        $this->assertEquals('in_review', $mrData['state']);
        $this->assertCount(1, $mrData['approvals']);

        $mrData = $this->getSecondReviewerStorageApiClient()->mergeRequestApprove($mrId);
        $this->assertCount(2, $mrData['approvals']);
        $this->assertSame('approved', $mrData['state']);

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('You don\'t have access to the resource.');
        $client->mergeMergeRequest($mrId);
    }

    public function testReviewerCannotApproveOwnMR(): void
    {
        $reviewerClient = $this->getReviewerStorageApiClient();

        $this->branches = new DevBranches($reviewerClient);
        $defaultBranch = $this->branches->getDefaultBranch();

        $newBranch = $this->branches->createBranch($this->generateDescriptionForTestObject() . '_aaaa');

        $mrId = $reviewerClient->createMergeRequest([
            'branchFromId' => $newBranch['id'],
            'branchIntoId' => $defaultBranch['id'],
            'title' => 'Change everything',
            'description' => 'Fix typo',
        ]);

        $reviewerClient->mergeRequestRequestReview($mrId);

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('Operation canot be performed due: Request creator cannot approve their own request.');
        $reviewerClient->mergeRequestApprove($mrId);
    }

    public function cantMergeTokenProviders(): Generator
    {
        yield 'developer' => [
            $this->getDeveloperStorageApiClient(),
        ];
        yield 'reviewer' => [
            $this->getReviewerStorageApiClient(),
        ];
        yield 'readOnly' => [
            $this->getReadOnlyStorageApiClient(),
        ];
    }

    public function testMrWithConflictCantBeMergedButAfterResetCan(): void
    {
        $componentId = 'wr-db';
        $configurationId = 'main-1';
        $components = new Components($this->getDefaultBranchStorageApiClient());

        $configuration = (new Configuration())
            ->setComponentId($componentId)
            ->setConfigurationId($configurationId)
            ->setName('Main')
            ->setDescription('some desc');
        $nonUpdatedConfig = $components->addConfiguration($configuration);

        $nonUpdatedConfigIdentifier = $nonUpdatedConfig['currentVersion']['versionIdentifier'];
        [$mrId, $branchId] = $this->createBranchMergeRequestAndApproveIt();
        // in default and dev branch is the same config with the same versionIdentifier

        $devBranchComponents = new Components($this->getBranchAwareClient($branchId, [
            'token' => STORAGE_API_DEVELOPER_TOKEN,
            'url' => STORAGE_API_URL,
        ]));
        $configInDevBranch = $devBranchComponents->getConfiguration($componentId, $configurationId);
        $this->assertSame($nonUpdatedConfigIdentifier, $configInDevBranch['currentVersion']['versionIdentifier']);
        // make change in default branch to create conflict
        $components->addConfigurationRow((new ConfigurationRow($configuration))
            ->setRowId('firstRow')
            ->setConfiguration(['value' => 1]));

        $actualIdentifierInMain = $components->getConfiguration($componentId, $configurationId);
        $actualIdentifierInBranch = $devBranchComponents->getConfiguration($componentId, $configurationId);
        $this->assertNotSame(
            $actualIdentifierInBranch['currentVersion']['versionIdentifier'],
            $actualIdentifierInMain['currentVersion']['versionIdentifier']
        );
        try {
            $this->prodManagerClient->mergeMergeRequest($mrId);
            $this->fail('Should fail, MR has conflict.');
        } catch (ClientException $e) {
            $this->assertSame(
                sprintf('Merge request %s cannot be merged. Problem with following configurations: componentId: "wr-db", configurationId: "main-1"', $mrId),
                $e->getMessage()
            );
        }
        $mr = $this->developerClient->getMergeRequest($mrId);
        $this->assertEquals('approved', $mr['state']);

        $devBranchComponents->resetToDefault($componentId, $configurationId);

        $actualIdentifierInMain = $components->getConfiguration($componentId, $configurationId);
        $actualIdentifierInBranch = $devBranchComponents->getConfiguration($componentId, $configurationId);
        $this->assertSame(
            $actualIdentifierInBranch['currentVersion']['versionIdentifier'],
            $actualIdentifierInMain['currentVersion']['versionIdentifier']
        );
        $this->initEvents($this->getDefaultBranchStorageApiClient());
        // todo now is works like this, but maybe it should go through approval process again
        $this->prodManagerClient->mergeMergeRequest($mrId);
        $mr = $this->developerClient->getMergeRequest($mrId);
        $this->assertEquals('published', $mr['state']);

        $this->assertBranchIsDeleted($branchId);
    }

    public function testConfigIsUpdatedInDefaultButBothConfigsAreDeleted(): void
    {
        $componentId = 'wr-db';
        $configurationId = 'main-1';
        $components = new Components($this->getDefaultBranchStorageApiClient());

        $configuration = (new Configuration())
            ->setComponentId($componentId)
            ->setConfigurationId($configurationId)
            ->setName('Main')
            ->setDescription('some desc');
        $components->addConfiguration($configuration);

        [$mrId, $branchId] = $this->createBranchMergeRequestAndApproveIt();
        // in default and dev branch is the same config with the same versionIdentifier

        // make change in default branch to create conflict
        $components->addConfigurationRow((new ConfigurationRow($configuration))
            ->setRowId('firstRow')
            ->setConfiguration(['value' => 1]));

        // Delete in default branch
        $components->deleteConfiguration($componentId, $configurationId);

        try {
            $this->prodManagerClient->mergeMergeRequest($mrId);
            $this->fail('Should fail, MR has conflict.');
        } catch (ClientException $e) {
            $this->assertSame(
                $e->getMessage(),
                sprintf('Merge request %s cannot be merged. Problem with following configurations: componentId: "wr-db", configurationId: "main-1"', $mrId)
            );
        }

        $devBranchComponents = new Components($this->getBranchAwareClient($branchId, [
            'token' => STORAGE_API_DEVELOPER_TOKEN,
            'url' => STORAGE_API_URL,
        ]));

        $devBranchComponents->deleteConfiguration($componentId, $configurationId);
        $this->initEvents($this->getDefaultBranchStorageApiClient());
        $this->prodManagerClient->mergeMergeRequest($mrId);
        $mr = $this->developerClient->getMergeRequest($mrId);
        $this->assertEquals('published', $mr['state']);

        $this->assertBranchIsDeleted($branchId);
    }

    public function testConfigurationUpdatedInBranch(): void
    {
        $defaultBranch = $this->branches->getDefaultBranch();

        // Create config in default branch
        /** @var Components $components */
        [$componentId, $configurationId, $components] = $this->prepareTestConfiguration();

        // create dev branch, config from main copy to dev
        $newBranch = $this->branches->createBranch($this->generateDescriptionForTestObject() . '_aaa');

        $devBranchComponents = new Components($this->getBranchAwareClient($newBranch['id'], [
            'token' => STORAGE_API_DEVELOPER_TOKEN,
            'url' => STORAGE_API_URL,
        ]));

        // check that the universe is OK and the configuration has been copied to the dev branch
        $configInDev = $devBranchComponents->getConfiguration($componentId, $configurationId);
        $this->assertSame('value', $configInDev['configuration']['main']);
        $this->assertSame(1, $configInDev['version']);
        $this->assertSame('Copied from default branch configuration "Main" (main-1) version 1', $configInDev['changeDescription']);

        // update existing config several times in default branch to check that only one version is added after the merge
        $devBranchComponents->updateConfiguration((new Configuration())
            ->setComponentId($componentId)
            ->setConfigurationId($configurationId)
            ->setName('Main updated')
            ->setDescription('First update description')
            ->setConfiguration(['main' => 'update'])
            ->setChangeDescription('Update config')
            ->setIsDisabled(true));

        $configInDev = $devBranchComponents->getConfiguration($componentId, $configurationId);
        $this->assertSame('update', $configInDev['configuration']['main']);
        $this->assertSame(2, $configInDev['version']);
        $this->assertSame('Update config', $configInDev['changeDescription']);

        $devBranchComponents->updateConfiguration((new Configuration())
            ->setComponentId($componentId)
            ->setConfigurationId($configurationId)
            ->setName('second main updated')
            ->setDescription('last update desc')
            ->setChangeDescription('last update')
            ->setConfiguration(['main' => 'update again']));

        $configState = (new ConfigurationState())
            ->setComponentId($componentId)
            ->setConfigurationId($configurationId)
            ->setState(['dev-branch-state' => 'state'])
        ;

        $devBranchComponents->updateConfigurationState($configState);

        $configInDev = $devBranchComponents->getConfiguration($componentId, $configurationId);
        $this->assertSame('update again', $configInDev['configuration']['main']);
        $this->assertSame(3, $configInDev['version']);
        $this->assertSame('last update', $configInDev['changeDescription']);
        $this->assertSame(['dev-branch-state' => 'state'], $configInDev['state']);

        $lastVersionIdentifierInDevBranch = $configInDev['currentVersion']['versionIdentifier'];
        $this->initEvents($this->getDefaultBranchStorageApiClient());
        // and merge it
        $this->mergeDevBranchToProd($newBranch['id'], $defaultBranch['id']);

        $configInDefault = $components->getConfiguration($componentId, $configurationId);
        $this->assertSame('update again', $configInDefault['configuration']['main']);
        $this->assertSame(2, $configInDefault['version']);
        $this->assertSame('second main updated', $configInDefault['name']);
        $this->assertSame('last update desc', $configInDefault['description']);
        $this->assertSame(['main-state' => 'state'], $configInDefault['state']);
        $this->assertTrue($configInDefault['isDisabled']);
        $this->assertStringContainsString(
            sprintf(
                'Configuration merged from branch: "%s"',
                $this->generateDescriptionForTestObject() . '_aaa'
            ),
            $configInDefault['changeDescription']
        );
        $this->assertNotSame($lastVersionIdentifierInDevBranch, $configInDefault['currentVersion']['versionIdentifier']);
        $versions = $components->listConfigurationVersions((new ListConfigurationVersionsOptions())
            ->setComponentId($componentId)
            ->setConfigurationId($configurationId));
        $this->assertCount(2, $versions);

        $this->assertBranchIsDeleted($newBranch['id']);
    }

    public function testChangeLog(): void
    {
        $defaultBranch = $this->branches->getDefaultBranch();

        // Create config in default branch
        /** @var Components $components */
        [$componentId, $configurationId, $components] = $this->prepareTestConfiguration();

        // create dev branch, config from main copy to dev
        $newBranch = $this->branches->createBranch($this->generateDescriptionForTestObject() . '_aaa');

        $devBranchComponents = new Components($this->getBranchAwareClient($newBranch['id'], [
            'token' => STORAGE_API_DEVELOPER_TOKEN,
            'url' => STORAGE_API_URL,
        ]));

        // check that the universe is OK and the configuration has been copied to the dev branch
        $configInDev = $devBranchComponents->getConfiguration($componentId, $configurationId);
        $this->assertSame('value', $configInDev['configuration']['main']);
        $this->assertSame(1, $configInDev['version']);
        $this->assertSame('Copied from default branch configuration "Main" (main-1) version 1', $configInDev['changeDescription']);

        // update existing config several times in default branch to check that only one version is added after the merge
        $devBranchComponents->updateConfiguration((new Configuration())
            ->setComponentId($componentId)
            ->setConfigurationId($configurationId)
            ->setName('Main updated')
            ->setDescription('First update description')
            ->setConfiguration(['main' => 'update'])
            ->setChangeDescription('Update config')
            ->setIsDisabled(true));

        $configInDev = $devBranchComponents->getConfiguration($componentId, $configurationId);
        $this->assertSame('update', $configInDev['configuration']['main']);
        $this->assertSame(2, $configInDev['version']);
        $this->assertSame('Update config', $configInDev['changeDescription']);

        $devBranchComponents->updateConfiguration((new Configuration())
            ->setComponentId($componentId)
            ->setConfigurationId($configurationId)
            ->setName('second main updated')
            ->setDescription('last update desc')
            ->setChangeDescription('last update')
            ->setConfiguration(['main' => 'update again']));

        $configState = (new ConfigurationState())
            ->setComponentId($componentId)
            ->setConfigurationId($configurationId)
            ->setState(['dev-branch-state' => 'state'])
        ;

        $devBranchComponents->updateConfigurationState($configState);

        $configInDev = $devBranchComponents->getConfiguration($componentId, $configurationId);
        $this->assertSame('update again', $configInDev['configuration']['main']);
        $this->assertSame(3, $configInDev['version']);
        $this->assertSame('last update', $configInDev['changeDescription']);
        $this->assertSame(['dev-branch-state' => 'state'], $configInDev['state']);

        $lastVersionIdentifierInDevBranch = $configInDev['currentVersion']['versionIdentifier'];

        $newConfigId = $this->generateUniqueNameForString('new-config');
        $devBranchComponents->addConfiguration(
            (new Configuration())
                ->setComponentId($componentId)
                ->setConfigurationId($newConfigId)
                ->setConfiguration(['main' => 'created in branch'])
            ->setName($this->generateDescriptionForTestObject()),
        );

        $this->initEvents($this->getDefaultBranchStorageApiClient());

        $mrId = $this->developerClient->createMergeRequest([
            'branchFromId' => $newBranch['id'],
            'branchIntoId' => $defaultBranch['id'],
            'title' => $this->generateUniqueNameForString('mr-title'),
            'description' => $this->generateUniqueNameForString('mr-description'),
        ]);

        $reviewerClient = $this->getReviewerStorageApiClient();
        $this->developerClient->mergeRequestRequestReview($mrId);
        $mr = $reviewerClient->getMergeRequest($mrId);
        $this->assertArrayHasKey('changeLog', $mr);
        $this->assertArrayHasKey('configurations', $mr['changeLog']);
        $this->assertCount(2, $mr['changeLog']['configurations']);
        foreach ($mr['changeLog']['configurations'] as $changedConfiguration) {
            $this->assertArrayHasKey('isDeleted', $changedConfiguration);
            $this->assertArrayHasKey('componentId', $changedConfiguration);
            $this->assertArrayHasKey('configurationId', $changedConfiguration);
            $this->assertArrayHasKey('lastVersionIdentifier', $changedConfiguration);
            $config = $devBranchComponents->getConfiguration(
                $changedConfiguration['componentId'],
                $changedConfiguration['configurationId']
            );
            $this->assertSame($config['currentVersion']['versionIdentifier'], $changedConfiguration['lastVersionIdentifier']);
        }

        $reviewerClient->requestMergeRequestChanges($mrId);

        $newConfigId = $this->generateUniqueNameForString('new-config-after-review');
        $devBranchComponents->addConfiguration(
            (new Configuration())
                ->setComponentId($componentId)
                ->setConfigurationId($newConfigId)
                ->setConfiguration(['main' => 'created in branch'])
                ->setName($this->generateDescriptionForTestObject()),
        );

        $this->developerClient->mergeRequestRequestReview($mrId);

        // reassert that the changelog is updated after review request
        $mr = $reviewerClient->getMergeRequest($mrId);
        $this->assertArrayHasKey('changeLog', $mr);
        $this->assertArrayHasKey('configurations', $mr['changeLog']);

        $this->assertCount(3, $mr['changeLog']['configurations']);
        foreach ($mr['changeLog']['configurations'] as $changedConfiguration) {
            $this->assertArrayHasKey('isDeleted', $changedConfiguration);
            $this->assertArrayHasKey('componentId', $changedConfiguration);
            $this->assertArrayHasKey('configurationId', $changedConfiguration);
            $this->assertArrayHasKey('lastVersionIdentifier', $changedConfiguration);
            $config = $devBranchComponents->getConfiguration(
                $changedConfiguration['componentId'],
                $changedConfiguration['configurationId']
            );
            $this->assertSame($config['currentVersion']['versionIdentifier'], $changedConfiguration['lastVersionIdentifier']);
        }
    }

    public function testCreateConfigurationInBranch(): void
    {
        $defaultBranch = $this->branches->getDefaultBranch();

        // Create config in default branch
        [$componentId, $configurationId, $components] = $this->prepareTestConfiguration();

        // create dev branch, config from main copy to dev
        $newBranch = $this->branches->createBranch($this->generateDescriptionForTestObject() . '_aaa');

        $devBranchComponents = new Components($this->getBranchAwareClient($newBranch['id'], [
            'token' => STORAGE_API_DEVELOPER_TOKEN,
            'url' => STORAGE_API_URL,
        ]));

        // check that the universe is OK and the configuration has been copied to the dev branch
        $configInDev = $devBranchComponents->getConfiguration($componentId, $configurationId);
        $this->assertSame('value', $configInDev['configuration']['main']);
        $this->assertSame(1, $configInDev['version']);
        $this->assertSame('Copied from default branch configuration "Main" (main-1) version 1', $configInDev['changeDescription']);
        $lastIdentifierInConfig1 = $configInDev['currentVersion']['versionIdentifier'];
        // create new config in dev branch
        $configuration = (new Configuration())
            ->setComponentId($componentId)
            ->setConfigurationId('config-in-dev-branch')
            ->setName('DevBranch')
            ->setDescription('dev config')
            ->setConfiguration(['dev' => 'value']);
        $devBranchComponents->addConfiguration($configuration);

        $configInDev = $devBranchComponents->getConfiguration($componentId, 'config-in-dev-branch');
        $this->assertSame('value', $configInDev['configuration']['dev']);
        $this->assertSame(1, $configInDev['version']);
        $this->assertSame('Configuration created', $configInDev['changeDescription']);
        $lastIdentifierInConfig2 = $configInDev['currentVersion']['versionIdentifier'];

        $configurationOptions = (new Configuration())
            ->setComponentId($componentId)
            ->setConfigurationId('config-in-dev-branch');
        $newRowInConfig = $devBranchComponents->addConfigurationRow((new ConfigurationRow($configurationOptions))
            ->setName('Row 1')
            ->setRowId('config-in-dev-branch-row-1'));
        $newRowIdentifier = $newRowInConfig['versionIdentifier'];

        $this->initEvents($this->getDefaultBranchStorageApiClient());
        // and merge it
        $this->mergeDevBranchToProd($newBranch['id'], $defaultBranch['id']);

        $configs = $components->listComponentConfigurations(
            (new ListComponentConfigurationsOptions())->setComponentId($componentId)
        );
        $this->assertCount(2, $configs);

        $firstConfigInDefault = $components->getConfiguration($componentId, $configurationId);
        $this->assertSame('value', $firstConfigInDefault['configuration']['main']);
        $this->assertSame(1, $firstConfigInDefault['version']);
        $this->assertSame('Configuration created', $firstConfigInDefault['changeDescription']);
        // if nothing updated in branch, nothing merged from branch and identifier is the same
        $this->assertEquals($lastIdentifierInConfig1, $firstConfigInDefault['currentVersion']['versionIdentifier']);

        $secondConfigInDefault = $components->getConfiguration($componentId, 'config-in-dev-branch');
        $this->assertSame('value', $secondConfigInDefault['configuration']['dev']);
        $this->assertSame(1, $secondConfigInDefault['version']);
        $this->assertStringContainsString(
            sprintf(
                'Configuration merged from branch: "%s"',
                $this->generateDescriptionForTestObject() . '_aaa'
            ),
            $secondConfigInDefault['changeDescription']
        );
        $this->assertSame('DevBranch', $secondConfigInDefault['name']);
        $this->assertSame('dev config', $secondConfigInDefault['description']);
        $this->assertFalse($secondConfigInDefault['isDisabled']);
        $this->assertNotEquals($lastIdentifierInConfig2, $secondConfigInDefault['currentVersion']['versionIdentifier']);
        $this->assertCount(1, $secondConfigInDefault['rows']);
        $this->assertNotEquals($newRowIdentifier, $secondConfigInDefault['rows'][0]['versionIdentifier']);

        $this->assertBranchIsDeleted($newBranch['id']);
    }

    public function testUpdateRow(): void
    {
        $defaultBranch = $this->branches->getDefaultBranch();

        // Create config in default branch
        /** @var Components $components */
        [$componentId, $configurationId, $components] = $this->prepareTestConfiguration();

        $configuration = (new Configuration())
            ->setComponentId($componentId)
            ->setConfigurationId($configurationId);
        $components->addConfigurationRow((new ConfigurationRow($configuration))
            ->setRowId('new-row')
            ->setConfiguration(['value' => 'row values']));

        $components->updateConfigurationRowState((new ConfigurationRowState($configuration))
            ->setRowId('new-row')
            ->setState(['main-row-state' => 'state']));
        $newBranch = $this->branches->createBranch($this->generateDescriptionForTestObject() . '_aaa');

        $devBranchComponents = new Components($this->getBranchAwareClient($newBranch['id'], [
            'token' => STORAGE_API_DEVELOPER_TOKEN,
            'url' => STORAGE_API_URL,
        ]));

        $rowsInDefault = $components->listConfigurationRows((new ListConfigurationRowsOptions())
            ->setComponentId($componentId)
            ->setConfigurationId($configurationId));
        $this->assertCount(1, $rowsInDefault);
        $this->assertSame('row values', $rowsInDefault[0]['configuration']['value']);
        $this->assertSame(1, $rowsInDefault[0]['version']);
        $this->assertSame(['main-row-state' => 'state'], $rowsInDefault[0]['state']);

        $configsInBranch = $devBranchComponents->listComponentConfigurations((new ListComponentConfigurationsOptions())->setComponentId($componentId));
        $this->assertCount(1, $configsInBranch);
        $rowsInBranch = $devBranchComponents->listConfigurationRows((new ListConfigurationRowsOptions())
            ->setComponentId($componentId)
            ->setConfigurationId($configurationId));
        $this->assertCount(1, $rowsInBranch);
        $this->assertSame('row values', $rowsInBranch[0]['configuration']['value']);
        $this->assertSame(1, $rowsInBranch[0]['version']);

        $devBranchComponents->updateConfigurationRow((new ConfigurationRow($configuration))
            ->setRowId('new-row')
            ->setConfiguration(['value' => 'row values updated'])
            ->setName('first update name')
            ->setDescription('first update'));

        $devBranchComponents->updateConfigurationRow((new ConfigurationRow($configuration))
            ->setRowId('new-row')
            ->setConfiguration(['value' => 'final update'])
            ->setName('second update name')
            ->setDescription('second update'));

        $devBranchComponents->updateConfigurationRowState((new ConfigurationRowState($configuration))
            ->setRowId('new-row')
            ->setState(['dev-branch-row-state' => 'state']));
        $updatedRow = $devBranchComponents->getConfigurationRow($componentId, $configurationId, 'new-row');
        $this->assertSame('final update', $updatedRow['configuration']['value']);
        $this->assertSame(3, $updatedRow['version']);
        $this->assertSame(['dev-branch-row-state' => 'state'], $updatedRow['state']);
        $lastVersionIdentifierInDevBranch = $updatedRow['versionIdentifier'];

        $this->initEvents($this->getDefaultBranchStorageApiClient());
        $this->mergeDevBranchToProd($newBranch['id'], $defaultBranch['id']);

        $rowInDefault = $components->getConfigurationRow($componentId, $configurationId, 'new-row');
        $this->assertSame('second update name', $rowInDefault['name']);
        $this->assertSame('second update', $rowInDefault['description']);
        $this->assertSame('final update', $rowInDefault['configuration']['value']);
        $this->assertSame(2, $rowInDefault['version']);
        $this->assertSame(['main-row-state' => 'state'], $rowsInDefault[0]['state']);
        $this->assertNotSame($lastVersionIdentifierInDevBranch, $rowInDefault['versionIdentifier']);
        $versions = $components->listConfigurationRowVersions((new ListConfigurationRowVersionsOptions())
            ->setComponentId('wr-db')
            ->setConfigurationId('main-1')
            ->setRowId('new-row'));
        $this->assertCount(2, $versions);

        $configInDefault = $components->getConfiguration($componentId, $configurationId);
        $this->assertStringContainsString(
            sprintf(
                'Configuration merged from branch: "%s"',
                $this->generateDescriptionForTestObject() . '_aaa'
            ),
            $configInDefault['changeDescription']
        );
        $versions = $components->listConfigurationVersions((new ListConfigurationVersionsOptions())
            ->setComponentId($componentId)
            ->setConfigurationId($configurationId));
        $this->assertCount(3, $versions);

        $this->assertBranchIsDeleted($newBranch['id']);
    }

    public function testAddRow(): void
    {
        $defaultBranch = $this->branches->getDefaultBranch();

        // Create config in default branch
        [$componentId, $configurationId, $components] = $this->prepareTestConfiguration();

        $configuration = (new Configuration())
            ->setComponentId($componentId)
            ->setConfigurationId($configurationId);
        $components->addConfigurationRow((new ConfigurationRow($configuration))
            ->setRowId('new-row')
            ->setConfiguration(['value' => 'row values']));

        $newBranch = $this->branches->createBranch($this->generateDescriptionForTestObject() . '_aaa');

        $devBranchComponents = new Components($this->getBranchAwareClient($newBranch['id'], [
            'token' => STORAGE_API_DEVELOPER_TOKEN,
            'url' => STORAGE_API_URL,
        ]));
        $rowsInDefault = $components->listConfigurationRows((new ListConfigurationRowsOptions())
            ->setComponentId($componentId)
            ->setConfigurationId($configurationId));
        $this->assertCount(1, $rowsInDefault);
        $this->assertSame(1, $rowsInDefault[0]['version']);

        $configsInBranch = $devBranchComponents->listComponentConfigurations((new ListComponentConfigurationsOptions())->setComponentId($componentId));
        $this->assertCount(1, $configsInBranch);
        $rowsInBranch = $devBranchComponents->listConfigurationRows((new ListConfigurationRowsOptions())
            ->setComponentId($componentId)
            ->setConfigurationId($configurationId));
        $this->assertCount(1, $rowsInBranch);

        $devBranchComponents->addConfigurationRow((new ConfigurationRow($configuration))
            ->setRowId('new-row-2')
            ->setConfiguration(['value' => 'row2 values updated'])
            ->setName('create row')
            ->setDescription('description'));

        $rowsInBranch = $devBranchComponents->listConfigurationRows((new ListConfigurationRowsOptions())
            ->setComponentId($componentId)
            ->setConfigurationId($configurationId));
        $this->assertCount(2, $rowsInBranch);

        $configInDev = $devBranchComponents->getConfiguration($componentId, $configurationId);
        $lastIdentifierInConfig = $configInDev['currentVersion']['versionIdentifier'];
        $row1 = $components->getConfigurationRow($componentId, $configurationId, 'new-row');
        $lastRow1Identifier = $row1['versionIdentifier'];

        $row2 = $devBranchComponents->getConfigurationRow($componentId, $configurationId, 'new-row-2');
        $lastRow2Identifier = $row2['versionIdentifier'];

        $this->initEvents($this->getDefaultBranchStorageApiClient());
        $this->mergeDevBranchToProd($newBranch['id'], $defaultBranch['id']);

        $rowsInDefault = $components->listConfigurationRows((new ListConfigurationRowsOptions())
            ->setComponentId($componentId)
            ->setConfigurationId($configurationId));
        $this->assertCount(2, $rowsInDefault);

        $row1 = $components->getConfigurationRow($componentId, $configurationId, 'new-row');
        $this->assertSame(1, $row1['version']);
        $this->assertEquals($lastRow1Identifier, $row1['versionIdentifier']);// no chage in dev branch -> same identifier

        $row2 = $components->getConfigurationRow($componentId, $configurationId, 'new-row-2');
        $this->assertSame(1, $row2['version']);
        $this->assertSame(['value' => 'row2 values updated'], $row2['configuration']);
        $this->assertSame('create row', $row2['name']);
        $this->assertSame('description', $row2['description']);
        $this->assertNotEquals($lastRow2Identifier, $row2['versionIdentifier']);

        $configInDev = $components->getConfiguration($componentId, $configurationId);
        $this->assertNotEquals($lastIdentifierInConfig, $configInDev['currentVersion']['versionIdentifier']);

        $this->assertBranchIsDeleted($newBranch['id']);
    }

    public function testRowsSortOrder(): void
    {
        $defaultBranch = $this->branches->getDefaultBranch();

        // Create config in default branch
        /** @var Components $components */
        [$componentId, $configurationId, $components] = $this->prepareTestConfiguration();
        $components->addConfigurationRow((new ConfigurationRow((new Configuration())
            ->setComponentId($componentId)
            ->setConfigurationId($configurationId)))
            ->setRowId('new-row')
            ->setConfiguration(['value' => 'row values']));

        $components->addConfigurationRow((new ConfigurationRow((new Configuration())
            ->setComponentId($componentId)
            ->setConfigurationId($configurationId)))
            ->setRowId('new-row-2')
            ->setConfiguration(['value' => 'row2 values updated'])
            ->setName('create row')
            ->setDescription('description'));

        $components->updateConfiguration((new Configuration())
            ->setComponentId($componentId)
            ->setConfigurationId($configurationId)
            ->setRowsSortOrder(['new-row-2', 'new-row']));
        $config = $components->getConfiguration($componentId, $configurationId);
        $this->assertSame(['new-row-2', 'new-row'], $config['rowsSortOrder']);

        // create dev branch, config from main copy to dev
        $newBranch = $this->branches->createBranch($this->generateDescriptionForTestObject() . '_aaa');

        $devBranchComponents = new Components($this->getBranchAwareClient($newBranch['id'], [
            'token' => STORAGE_API_DEVELOPER_TOKEN,
            'url' => STORAGE_API_URL,
        ]));

        // check that the universe is OK and the configuration has been copied to the dev branch
        $configInDev = $devBranchComponents->getConfiguration($componentId, $configurationId);
        $this->assertSame('value', $configInDev['configuration']['main']);
        $this->assertSame(1, $configInDev['version']);
        $this->assertSame('Copied from default branch configuration "Main" (main-1) version 4', $configInDev['changeDescription']);
        $this->assertSame(['new-row-2', 'new-row'], $configInDev['rowsSortOrder']);

        $row = $devBranchComponents->getConfigurationRow($componentId, $configurationId, 'new-row');
        $this->assertSame('row values', $row['configuration']['value']);
        $this->assertSame(1, $row['version']);
        $this->assertSame('Copied from default branch configuration row "" (new-row) version 1', $row['changeDescription']);

        $devBranchComponents->addConfigurationRow((new ConfigurationRow((new Configuration())
            ->setComponentId($componentId)
            ->setConfigurationId($configurationId)))
            ->setRowId('dev-row')
            ->setConfiguration(['value' => 'row values'])
            ->setDescription('description'));

        $devBranchComponents->updateConfiguration((new Configuration())
            ->setComponentId($componentId)
            ->setConfigurationId($configurationId)
            ->setRowsSortOrder(['new-row', 'dev-row', 'new-row-2']));
        $config = $devBranchComponents->getConfiguration($componentId, $configurationId);
        $this->assertSame(['new-row', 'dev-row', 'new-row-2'], $config['rowsSortOrder']);

        $this->initEvents($this->getDefaultBranchStorageApiClient());
        $this->mergeDevBranchToProd($newBranch['id'], $defaultBranch['id']);

        $config = $components->getConfiguration($componentId, $configurationId);
        $this->assertSame(['new-row', 'dev-row', 'new-row-2'], $config['rowsSortOrder']);

        $rowsInDefault = $components->listConfigurationRows((new ListConfigurationRowsOptions())
            ->setComponentId($componentId)
            ->setConfigurationId($configurationId));
        $this->assertCount(3, $rowsInDefault);

        $this->assertBranchIsDeleted($newBranch['id']);
    }

    public function testCopyMetadataAfterMerge(): void
    {
        $defaultBranch = $this->branches->getDefaultBranch();

        // Create config in default branch
        /** @var Components $components */
        [$componentId, $configurationId, $components] = $this->prepareTestConfiguration();

        $testMetadata = [
            [
                'key' => 'KBC.SomeEnity.metadataKey',
                'value' => 'some-value',
            ],
            [
                'key' => 'someMetadataKey',
                'value' => 'some-value',
            ],
        ];
        $configurationOptions = (new Configuration())
            ->setComponentId($componentId)
            ->setConfigurationId($configurationId);

        $configurationMetadataOptions = (new ConfigurationMetadata($configurationOptions))
            ->setMetadata($testMetadata);
        $components->addConfigurationMetadata($configurationMetadataOptions);

        $configuration = (new Configuration())
            ->setComponentId($componentId)
            ->setConfigurationId('main-2')
            ->setName('Main 2');
        $components->addConfiguration($configuration);

        // create dev branch, config from main copy to dev
        $newBranch = $this->branches->createBranch($this->generateDescriptionForTestObject() . '_aaa');

        $devBranchComponents = new Components($this->getBranchAwareClient($newBranch['id'], [
            'token' => STORAGE_API_DEVELOPER_TOKEN,
            'url' => STORAGE_API_URL,
        ]));

        // check that the universe is OK and the configuration has been copied to the dev branch
        $configInDev = $devBranchComponents->getConfiguration($componentId, $configurationId);
        $this->assertSame('value', $configInDev['configuration']['main']);
        $this->assertSame(1, $configInDev['version']);
        $this->assertSame('Copied from default branch configuration "Main" (main-1) version 1', $configInDev['changeDescription']);
        $listConfigurationMetadata = $components->listConfigurationMetadata((new ListConfigurationMetadataOptions())
            ->setComponentId($componentId)
            ->setConfigurationId($configurationId));
        $this->assertCount(2, $listConfigurationMetadata);

        $configInDev = $devBranchComponents->getConfiguration($componentId, 'main-2');
        $this->assertEmpty($configInDev['configuration']);
        $this->assertSame(1, $configInDev['version']);
        $this->assertSame('Copied from default branch configuration "Main 2" (main-2) version 1', $configInDev['changeDescription']);
        $listConfigurationMetadata = $devBranchComponents->listConfigurationMetadata((new ListConfigurationMetadataOptions())
            ->setComponentId($componentId)
            ->setConfigurationId('main-2'));
        $this->assertCount(0, $listConfigurationMetadata);

        $testMetadataConfig2 = [
            [
                'key' => 'KBC.SomeEnity.metadataKey',
                'value' => 'second-value',
            ],
            [
                'key' => 'someMetadataKey',
                'value' => 'second-value',
            ],
        ];
        $configurationOptions = (new Configuration())
            ->setComponentId($componentId)
            ->setConfigurationId('main-2');

        $configurationMetadataOptions = (new ConfigurationMetadata($configurationOptions))
            ->setMetadata($testMetadataConfig2);
        $devBranchComponents->addConfigurationMetadata($configurationMetadataOptions);

        $listConfigurationMetadata = $devBranchComponents->listConfigurationMetadata((new ListConfigurationMetadataOptions())
            ->setComponentId($componentId)
            ->setConfigurationId('main-2'));
        $this->assertCount(2, $listConfigurationMetadata);

        $updatedMetadata = [
            [
                'key' => 'someMetadataKey',
                'value' => 'updated-value',
            ],
        ];
        $configurationOptions = (new Configuration())
            ->setComponentId($componentId)
            ->setConfigurationId($configurationId);
        $configurationMetadataOptions = (new ConfigurationMetadata($configurationOptions))
            ->setMetadata($updatedMetadata);
        $md = $devBranchComponents->addConfigurationMetadata($configurationMetadataOptions);
        $this->assertMetadataEquals($testMetadata[0], $md[0]);
        $this->assertMetadataEquals($updatedMetadata[0], $md[1]);

        $this->initEvents($this->getDefaultBranchStorageApiClient());
        $this->mergeDevBranchToProd($newBranch['id'], $defaultBranch['id']);

        $listConfigurationMetadata = $components->listConfigurationMetadata((new ListConfigurationMetadataOptions())
            ->setComponentId($componentId)
            ->setConfigurationId($configurationId));
        $this->assertCount(2, $listConfigurationMetadata);
        $this->assertMetadataEquals($testMetadata[0], $listConfigurationMetadata[0]);
        $this->assertMetadataEquals($updatedMetadata[0], $listConfigurationMetadata[1]);

        $listConfigurationMetadata = $components->listConfigurationMetadata((new ListConfigurationMetadataOptions())
            ->setComponentId($componentId)
            ->setConfigurationId('main-2'));
        $this->assertCount(2, $listConfigurationMetadata);
        $this->assertMetadataEquals($testMetadataConfig2[0], $listConfigurationMetadata[0]);
        $this->assertMetadataEquals($testMetadataConfig2[1], $listConfigurationMetadata[1]);

        $this->assertBranchIsDeleted($newBranch['id']);
    }

    public function testDeleteConfiguration(): void
    {
        $defaultBranch = $this->branches->getDefaultBranch();

        // Create config in default branch
        /** @var Components $components */
        [$componentId, $configurationId, $components] = $this->prepareTestConfiguration();

        // create dev branch, config from main copy to dev
        $newBranch = $this->branches->createBranch($this->generateDescriptionForTestObject() . '_aaa');

        $devBranchComponents = new Components($this->getBranchAwareClient($newBranch['id'], [
            'token' => STORAGE_API_DEVELOPER_TOKEN,
            'url' => STORAGE_API_URL,
        ]));

        // check that the universe is OK and the configuration has been copied to the dev branch
        $configInDev = $devBranchComponents->getConfiguration($componentId, $configurationId);
        $this->assertSame('value', $configInDev['configuration']['main']);
        $this->assertSame(1, $configInDev['version']);
        $this->assertSame('Copied from default branch configuration "Main" (main-1) version 1', $configInDev['changeDescription']);

        $devBranchComponents->deleteConfiguration($componentId, $configurationId);
        try {
            $devBranchComponents->getConfiguration($componentId, $configurationId);
            $this->fail('Configuration should not exist');
        } catch (ClientException $e) {
            $this->assertSame(404, $e->getCode());
        }

        try {
            $devBranchComponents->deleteConfiguration($componentId, $configurationId);
            $this->fail('Deleting configuration from trash is not allowed in development branches.');
        } catch (ClientException $e) {
            $this->assertSame(400, $e->getCode());
            $this->assertSame('Deleting configuration from trash is not allowed in development branches.', $e->getMessage());
        }

        $this->initEvents($this->getDefaultBranchStorageApiClient());
        $this->mergeDevBranchToProd($newBranch['id'], $defaultBranch['id']);

        try {
            $components->getConfiguration($componentId, $configurationId);
            $this->fail('Configuration should not exist');
        } catch (ClientException $e) {
            $this->assertSame(404, $e->getCode());
        }

        $this->assertBranchIsDeleted($newBranch['id']);
    }

    public function testMergerRequestIsCanceledWhenBranchIsDeleted(): void
    {
        $createMr = function () {
            $defaultBranch = $this->branches->getDefaultBranch();
            $newBranch = $this->branches->createBranch($this->generateDescriptionForTestObject() . '_aaaa');
            $mrId = $this->developerClient->createMergeRequest([
                'branchFromId' => $newBranch['id'],
                'branchIntoId' => $defaultBranch['id'],
                'title' => 'Change everything',
                'description' => 'Fix typo',
            ]);
            return [$mrId, $newBranch['id']];
        };

        // state in development
        [$mrId, $branchId] = $createMr();
        $this->initEvents($this->getDefaultBranchStorageApiClient());
        (new DevBranches($this->developerClient))->deleteBranch($branchId);
        $this->assertBranchIsDeleted($branchId);
        $mr = $this->developerClient->getMergeRequest($mrId);
        $this->assertSame('canceled', $mr['state']);

        // state in review
        [$mrId, $branchId] = $createMr();
        $this->developerClient->mergeRequestRequestReview($mrId);
        $this->initEvents($this->getDefaultBranchStorageApiClient());
        (new DevBranches($this->developerClient))->deleteBranch($branchId);
        $this->assertBranchIsDeleted($branchId);
        $mr = $this->developerClient->getMergeRequest($mrId);
        $this->assertSame('canceled', $mr['state']);

        // state approved
        [$mrId, $branchId] = $createMr();
        $this->developerClient->mergeRequestRequestReview($mrId);
        $this->getReviewerStorageApiClient()->mergeRequestApprove($mrId);
        $this->getSecondReviewerStorageApiClient()->mergeRequestApprove($mrId);
        $this->initEvents($this->getDefaultBranchStorageApiClient());
        (new DevBranches($this->developerClient))->deleteBranch($branchId);
        $this->assertBranchIsDeleted($branchId);
        $mr = $this->developerClient->getMergeRequest($mrId);
        $this->assertSame('canceled', $mr['state']);

        // state in development after changes requested
        [$mrId, $branchId] = $createMr();
        $this->developerClient->mergeRequestRequestReview($mrId);
        $this->getReviewerStorageApiClient()->requestMergeRequestChanges($mrId);
        $this->initEvents($this->getDefaultBranchStorageApiClient());
        (new DevBranches($this->developerClient))->deleteBranch($branchId);
        $this->assertBranchIsDeleted($branchId);
        $mr = $this->developerClient->getMergeRequest($mrId);
        $this->assertSame('canceled', $mr['state']);
    }

    private function createBranchMergeRequestAndApproveIt(): array
    {
        $defaultBranch = $this->branches->getDefaultBranch();
        $newBranch = $this->branches->createBranch($this->generateDescriptionForTestObject() . '_aaaa');

        $mrId = $this->developerClient->createMergeRequest([
            'branchFromId' => $newBranch['id'],
            'branchIntoId' => $defaultBranch['id'],
            'title' => 'Change everything',
            'description' => 'Fix typo',
        ]);

        $reviewerClient = $this->getReviewerStorageApiClient();
        $this->developerClient->mergeRequestRequestReview($mrId);

        $reviewerClient->mergeRequestApprove($mrId);
        $this->getSecondReviewerStorageApiClient()->mergeRequestApprove($mrId);

        return [$mrId, $newBranch['id']];
    }

    /**
     * @return array
     */
    public function prepareTestConfiguration(): array
    {
        $componentId = 'wr-db';
        $configurationId = 'main-1';
        $components = new Components($this->getDefaultBranchStorageApiClient());

        $configuration = (new Configuration())
            ->setComponentId($componentId)
            ->setConfigurationId($configurationId)
            ->setName('Main')
            ->setDescription('main config')
            ->setConfiguration(['main' => 'value']);
        $components->addConfiguration($configuration);

        $configState = (new ConfigurationState())
            ->setComponentId($componentId)
            ->setConfigurationId($configurationId)
            ->setState(['main-state' => 'state'])
        ;

        $components->updateConfigurationState($configState);

        $configInDefault = $components->getConfiguration($componentId, $configurationId);
        $this->assertSame('value', $configInDefault['configuration']['main']);
        $this->assertSame(1, $configInDefault['version']);
        $this->assertSame('Configuration created', $configInDefault['changeDescription']);
        $this->assertSame(['main-state' => 'state'], $configInDefault['state']);
        $versions = $components->listConfigurationVersions((new ListConfigurationVersionsOptions())
            ->setComponentId($componentId)
            ->setConfigurationId($configurationId));
        $this->assertCount(1, $versions);
        return [$componentId, $configurationId, $components];
    }

    private function mergeDevBranchToProd(int $branchFromId, int $branchIntoId): void
    {
        $mrId = $this->developerClient->createMergeRequest([
            'branchFromId' => $branchFromId,
            'branchIntoId' => $branchIntoId,
            'title' => 'Change everything',
            'description' => 'Fix typo',
        ]);

        $reviewerClient = $this->getReviewerStorageApiClient();
        $this->developerClient->mergeRequestRequestReview($mrId);
        $reviewerClient->mergeRequestApprove($mrId);
        $this->getSecondReviewerStorageApiClient()->mergeRequestApprove($mrId);

        $this->initEvents($this->getDefaultBranchStorageApiClient());
        $this->prodManagerClient->mergeMergeRequest($mrId);
        $assertCallback = function ($events) {
            $this->assertCount(2, $events);
            $params = $events[0]['params'];
            unset($params['mergeRequestId']);
            $this->assertEquals([
                'operation' => 'publish',
                'stateFrom' => 'in_merge',
                'stateTo' => 'published',
            ], $params);
            $params = $events[1]['params'];
            unset($params['mergeRequestId']);
            $this->assertEquals([
                'operation' => 'merge',
                'stateFrom' => 'approved',
                'stateTo' => 'in_merge',
            ], $params);
        };
        $eventsQuery = new EventsQueryBuilder();
        $eventsQuery->setEvent('storage.mergeRequestStateChanged');
        $eventsQuery->setObjectId((string) $mrId);
        $eventsQuery->setObjectType('mergeRequest');
        $this->assertEventWithRetries($this->getDefaultClient(), $assertCallback, $eventsQuery);
    }

    /**
     * @param string|int $id
     */
    private function assertBranchIsDeleted($id): void
    {
        $this->assertEventWithRetries(
            $this->getDeveloperStorageApiClient(),
            function ($events) use ($id) {
                $this->assertCount(1, $events);
                $this->assertSame('storage.devBranchDeleted', $events[0]['event']);
                $this->assertSame($id, $events[0]['objectId']);
            },
            (new EventsQueryBuilder())
                ->setEvent('storage.devBranchDeleted')
                ->setObjectId((string) $id)
        );

        $devBranch = new DevBranches($this->developerClient);
        try {
            $devBranch->getBranch((int) $id);
            $this->fail(sprintf('Branch id:"%s" should not exist.', $id));
        } catch (ClientException $e) {
            $this->assertSame(404, $e->getCode());
            $this->assertSame(sprintf('Branch id:"%s" was not found.', $id), $e->getMessage());
        }
    }
}
