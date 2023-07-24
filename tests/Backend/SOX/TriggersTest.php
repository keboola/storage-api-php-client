<?php

namespace Keboola\Test\Backend\SOX;

use Generator;
use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Keboola\Test\StorageApiTestCase;

class TriggersTest extends StorageApiTestCase
{
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
}
