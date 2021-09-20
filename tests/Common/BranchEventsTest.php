<?php

namespace Keboola\Test\Common;

use GuzzleHttp\Exception\RequestException;
use GuzzleHttp\Exception\ServerException;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\DevBranches;
use Keboola\Test\StorageApiTestCase;
use Keboola\StorageApi\Event;

class BranchEventsTest extends StorageApiTestCase
{
    public function testDevBranchEventCreated()
    {
        $providedToken = $this->_client->verifyToken();
        $devBranch = new DevBranches($this->_client);
        // cleanup
        $branchesList = $devBranch->listBranches();
        $branchName = __CLASS__ . '\\' . $this->getName() . '\\' . $providedToken['id'];
        $branchesCreatedByThisTestMethod = array_filter(
            $branchesList,
            function ($branch) use ($branchName) {
                return strpos($branch['name'], $branchName) === 0;
            }
        );
        foreach ($branchesCreatedByThisTestMethod as $branch) {
            try {
                $this->_client->dropBucket('in.c-dev-branch-' . $branch['id']);
            } catch (ClientException $e) {
            }

            $devBranch->deleteBranch($branch['id']);
        }

        // event for main branch dispatched
        $branch = $devBranch->createBranch($branchName);
        $configurationId = 'config-id-dev-branch-' . $branch['id'];

        $branchAwareClient = $this->getBranchAwareDefaultClient($branch['id']);

        $branchComponents = new \Keboola\StorageApi\Components($branchAwareClient);
        $config = (new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('transformation')
            ->setConfigurationId($configurationId)
            ->setName('Dev Branch 1')
            ->setDescription('Configuration created');

        // event for development branch dispatched
        $branchComponents->addConfiguration($config);


        // create dummy config to test only one event return from $branchAwareClient
        $dummyBranch = $devBranch->createBranch($branchName . '-dummy');
        $dummyBranchAwareClient = $this->getBranchAwareDefaultClient($dummyBranch['id']);
        $dummyConfigurationId = 'dummy-config-id-dev-branch-' . $branch['id'];

        $dummyBranchComponents = new \Keboola\StorageApi\Components($dummyBranchAwareClient);
        $config = (new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('transformation')
            ->setConfigurationId($dummyConfigurationId)
            ->setName('Dummy Dev Branch 1')
            ->setDescription('Configuration created');

        // event for dummy branch dispatched
        $dummyBranchComponents->addConfiguration($config);

        // There could be more than one event because for example bucket events are also returned
        // Test if return only one event created before for branch
        $branchAwareEvents = $this->waitForListEvents(
            $branchAwareClient,
            'event:storage.componentConfigurationCreated'
        );

        $this->assertCount(1, $branchAwareEvents);

        $createConfigEventDetail = $branchAwareClient->getEvent($branchAwareEvents[0]['id']);

        $this->assertSame('config-id-dev-branch-'.$branch['id'], $createConfigEventDetail['objectId']);

        // test allowed non branch aware event - create bucket detail event in main branch
        $testBucketId = $this->_client->createBucket($configurationId, self::STAGE_IN);

        // event about bucket create should be return from branch aware event list
        $bucketsListedEvents = $this->waitForListEvents(
            $branchAwareClient,
            'objectId:' . $testBucketId
        );
        $this->assertCount(1, $bucketsListedEvents);
        $this->assertSame('storage.bucketCreated', $bucketsListedEvents[0]['event']);

        $bucketEventDetail = $branchAwareClient->getEvent($bucketsListedEvents[0]['id']);

        $this->assertSame($testBucketId, $bucketEventDetail['objectId']);

        // test create external event shows in branch aware events
        $event = new Event();
        $event->setComponent('ex-sfdc')
            ->setConfigurationId('sys.c-sfdc.account-'.$branch['id'])
            ->setDuration(200)
            ->setType('info')
            ->setRunId('ddddssss')
            ->setMessage('Table Opportunity fetched.')
            ->setDescription('Some longer description of event')
            ->setParams(array(
                'accountName' => 'Keboola',
                'configuration' => 'sys.c-sfdc.sfdc-01',
            ));
        $this->createAndWaitForEvent($event, $branchAwareClient);

        $bucketsListedEvents = $this->waitForListEvents(
            $branchAwareClient,
            'event:ext.ex-sfdc.sys.c-sfdc.account-'.$branch['id']
        );
        $this->assertCount(1, $bucketsListedEvents);
        $this->assertEquals('ex-sfdc', $bucketsListedEvents[0]['component']);
        $this->assertEquals('sys.c-sfdc.account-'.$branch['id'], $bucketsListedEvents[0]['configurationId']);
        $this->assertEquals(200, $bucketsListedEvents[0]['performance']['duration']);
        $this->assertEquals('info', $bucketsListedEvents[0]['type']);
        $this->assertEquals('ddddssss', $bucketsListedEvents[0]['runId']);
        $this->assertEquals('Table Opportunity fetched.', $bucketsListedEvents[0]['message']);
        $this->assertEquals('Some longer description of event', $bucketsListedEvents[0]['description']);
        $this->assertEquals([
            'accountName' => 'Keboola',
            'configuration' => 'sys.c-sfdc.sfdc-01',
        ], $bucketsListedEvents[0]['params']);
        $this->assertEquals($branch['id'], $bucketsListedEvents[0]['idBranch']);

        // check if there no exist componentConfigurationCreated event for main branch
        // to validate only main branch events will be returned
        $componentConfigCreateEvents = $this->_client->listEvents([
            'q' => 'objectId:' . $configurationId,
        ]);
        $this->assertCount(0, $componentConfigCreateEvents);

        $clientEventList = $this->_client->listEvents([
            'q' => 'idBranch:' . $branch['id']
        ]);
        $this->assertCount(0, $clientEventList);

        $bucketClientEventList = $this->_client->listEvents([
            'q' => 'objectId:' . $testBucketId
        ]);
        $this->assertCount(1, $bucketClientEventList);

        $this->assertTrue(count($this->_client->listEvents()) > 1);


        $components = new \Keboola\StorageApi\Components($this->_client);
        $config = (new \Keboola\StorageApi\Options\Components\Configuration())
            ->setComponentId('transformation')
            ->setConfigurationId('main-config-created-'.$branch['id'])
            ->setName('Main Branch 1')
            ->setDescription('Main Configuration created');

        // event for main branch dispatched
        $components->addConfiguration($config);

        $componentCreateInMainBranchListedEvents = $this->waitForListEvents(
            $this->_client,
            'objectId:main-config-created-'.$branch['id']
        );

        try {
            $branchAwareClient->getEvent($componentCreateInMainBranchListedEvents[0]['id']);
            $this->fail('Main branch aware event should not show for dev branch event');
        } catch (ClientException $e) {
            $this->assertSame(404, $e->getCode());
            $this->assertSame('Event not found', $e->getMessage());
        }
    }

    private function waitForListEvents(Client $client, $query)
    {
        sleep(2); // wait for ES refresh
        $tries = 0;
        while (true) {
            $list = $client->listEvents([
                'q' => $query,
            ]);
            if (count($list) > 0) {
                return $list;
            }
            if ($tries > 4) {
                throw new \Exception('Max tries exceeded.');
            }
            $tries++;
            sleep(pow(2, $tries));
        }
    }
}
