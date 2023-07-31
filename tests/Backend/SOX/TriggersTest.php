<?php

namespace Keboola\Test\Backend\SOX;

use Generator;
use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\DevBranches;
use Keboola\Test\StorageApiTestCase;
use Throwable;

class TriggersTest extends StorageApiTestCase
{
    private DevBranches $branches;

    public function setUp(): void
    {
        parent::setUp();
        $developerClient = $this->getDeveloperStorageApiClient();
        $this->branches = new DevBranches($developerClient);
        $this->cleanupTestBranches($developerClient);
    }

    public function testCreateTriggerInSOX(): void
    {
        $description = $this->generateDescriptionForTestObject();

        $privilegedClient = $this->getDefaultBranchStorageApiClient();
        $productionBucketId = $this->initEmptyBucket(
            $this->getTestBucketName($description),
            self::STAGE_IN,
            $description,
            $privilegedClient
        );
        $productionTableId = $privilegedClient->createTableAsync(
            $productionBucketId,
            'languages',
            new CsvFile(__DIR__ . '/../../_data/languages.csv')
        );

        $token = $this->getDefaultClient()->verifyToken();
        // as prod Manager
        $createdTrigger = $this->getDefaultClient()->createTrigger([
            'component' => 'keboola.orchestrator',
            'configurationId' => 123,
            'coolDownPeriodMinutes' => 10,
            'runWithTokenId' => $token['id'],
            'tableIds' => [
                $productionTableId,
            ],
        ]);

        $this->assertEquals($token['id'], $createdTrigger['runWithTokenId']);

        $updatedTrigger = $this->getDefaultClient()->updateTrigger($createdTrigger['id'], [
            'coolDownPeriodMinutes' => 11,
        ]);

        $this->assertEquals(11, $updatedTrigger['coolDownPeriodMinutes']);

        $triggerDetail = $this->getDefaultClient()->getTrigger($createdTrigger['id']);

        $this->assertEquals(11, $triggerDetail['coolDownPeriodMinutes']);

        $this->getDefaultClient()->deleteTrigger($createdTrigger['id']);

        $this->expectExceptionCode(404);
        $this->getDefaultClient()->getTrigger($createdTrigger['id']);
    }

    public function developerAndReviewerClientProvider(): Generator
    {
        yield 'developer' => [
            $this->getDeveloperStorageApiClient(),
        ];
        yield 'reviewer' => [
            $this->getReviewerStorageApiClient(),
        ];
    }

    /**
     * @dataProvider developerAndReviewerClientProvider
     */
    public function testOtherRolesCannotUpdateOrdeleteTriggerInSOX(Client $client): void
    {
        $description = $this->generateDescriptionForTestObject();

        $privilegedClient = $this->getDefaultBranchStorageApiClient();
        $productionBucketId = $this->initEmptyBucket(
            $this->getTestBucketName($description),
            self::STAGE_IN,
            $description,
            $privilegedClient
        );
        $productionTableId = $privilegedClient->createTableAsync(
            $productionBucketId,
            'languages',
            new CsvFile(__DIR__ . '/../../_data/languages.csv')
        );

        $token = $this->getDefaultClient()->verifyToken();
        // as prod Manager
        $createdTrigger = $this->getDefaultClient()->createTrigger([
            'component' => 'keboola.orchestrator',
            'configurationId' => 123,
            'coolDownPeriodMinutes' => 10,
            'runWithTokenId' => $token['id'],
            'tableIds' => [
                $productionTableId,
            ],
        ]);

        $getTrigger = $client->getTrigger($createdTrigger['id']);
        $this->assertEquals($createdTrigger['id'], $getTrigger['id']);

        // assert update
        try {
            $client->updateTrigger($createdTrigger['id'], [
                'component' => 'keboola.orchestrator',
                'configurationId' => 123,
                'coolDownPeriodMinutes' => 10,
                'runWithTokenId' => 1,
                'tableIds' => ['aaa'],
            ]);
            $this->fail('Should not be able to update trigger');
        } catch (\Exception $e) {
            $this->assertSame('You don\'t have access to the resource.', $e->getMessage());
        }

        // assert delete
        try {
            $client->deleteTrigger($createdTrigger['id']);
            $this->fail('Should not be able to delete trigger');
        } catch (\Exception $e) {
            $this->assertSame('You don\'t have access to the resource.', $e->getMessage());
        }

        // assert create
        try {
            $client->createTrigger([
                'component' => 'keboola.orchestrator',
                'configurationId' => 123,
                'coolDownPeriodMinutes' => 10,
                'runWithTokenId' => 1,
                'tableIds' => ['aaa'],
            ]);
            $this->fail('Should not be able to create trigger');
        } catch (\Exception $e) {
            $this->assertSame('You don\'t have access to the resource.', $e->getMessage());
        }
    }

    public function clientProvider(): Generator
    {
        yield 'nobody can create trigger in branch (privileged)' => [
            $this->getDefaultBranchStorageApiClient(),
        ];
        yield 'nobody can create trigger in branch (productionManager)' => [
            $this->getDefaultClient(),
        ];
        yield 'nobody can create trigger in branch (developer)' => [
            $this->getDeveloperStorageApiClient(),
        ];
        yield 'nobody can create trigger in branch (reviewer)' => [
            $this->getReviewerStorageApiClient(),
        ];
        yield 'nobody can create trigger in branch (readOnly)' => [
            $this->getReadOnlyStorageApiClient(),
        ];
    }

    /**
     * @dataProvider clientProvider
     */
    public function testTriggerCannotBeCreatedInBranch(Client $client): void
    {
        ['id' => $branchId] = (new DevBranches($this->getDeveloperStorageApiClient()))->createBranch($this->generateDescriptionForTestObject());

        $this->expectException(ClientException::class);
        $this->expectExceptionMessage('Not implemented');
        $this->expectExceptionCode(501);
        $client->getBranchAwareClient($branchId)->createTrigger([]);
    }

    public function testCreateTriggerInDefaultBranchWithTableInBranch(): void
    {
        $description = $this->generateDescriptionForTestObject();

        $newBranch = $this->branches->createBranch($this->generateDescriptionForTestObject());
        $developerClient = $this->getDeveloperStorageApiClient();
        $developerBranchClient = $developerClient->getBranchAwareClient($newBranch['id']);
        $branchBucketId = $this->initEmptyBucket(
            $this->getTestBucketName($description),
            self::STAGE_IN,
            $description,
            $developerBranchClient
        );
        $branchTableId = $developerBranchClient->createTableAsync(
            $branchBucketId,
            'languages',
            new CsvFile(__DIR__ . '/../../_data/languages.csv')
        );

        $token = $this->getDefaultClient()->verifyToken();
        try {
            $this->getDefaultClient()->createTrigger([
                'component' => 'keboola.orchestrator',
                'configurationId' => 123,
                'coolDownPeriodMinutes' => 10,
                'runWithTokenId' => $token['id'],
                'tableIds' => [
                    $branchTableId,
                ],
            ]);
        } catch (ClientException $e) {
            $this->assertSame($e->getCode(), 404);
            $this->assertSame(
                sprintf(
                    'The table "languages" was not found in the bucket "%s" in the project "%s"',
                    $branchBucketId,
                    $token['owner']['id']
                ),
                $e->getMessage()
            );
        }
    }
}
