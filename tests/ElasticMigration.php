<?php

declare(strict_types=1);

use Keboola\StorageApi\Event;
use Keboola\Test\StorageApiTestCase;

class ElasticMigration extends StorageApiTestCase
{
    // run with this in step "Step 2;1b"
    const COMPONENT_NAME = '01993999-2957-7daa-85a6-e69c1d7d2a20';
    const CONFIG_ID = '01993999-5a27-73bf-80ed-fdf1c85dd651';
    const TAG_NAME = '01993999-7799-7777-bb19-6d058a32d904';
    const BUCKET_NAME = '01993999-95f5-7604-a0bb-5b40b9167d84';

    // run with this in step "Step 2;3"
//    const COMPONENT_NAME = '01993999-bba9-7a39-b382-ed7014a56f9b';
//    const CONFIG_ID = '01993999-dbcb-7a37-a19e-ef5abc3e74e3';
//    const TAG_NAME = '01993999-f8bf-7d01-add1-12e69a04bffe';
//    const BUCKET_NAME = '0199399a-1576-762a-acd8-ee0c0a1b1717';

    public function testPrepareItems()
    {
        // Create all test entities
        $this->testCreateTestFile();
        $this->testCreateTestBucket();
        $this->testCreateTestEvent();
    }

    public function testAfterMigration()
    {
        // Run all entity-specific tests
        $this->testEventSearch();
        $this->testFileSearch();
        $this->testGlobalSearch();
    }

    public function testCreateTestFile(): int
    {
        $filePath = __DIR__ . '/_data/users.csv';
        $uploadOptions = new \Keboola\StorageApi\Options\FileUploadOptions();
        $uploadOptions->setTags([self::TAG_NAME]);

        $fileId = $this->createAndWaitForFile($filePath, $uploadOptions);
        $this->assertGreaterThan(0, $fileId, 'File should be uploaded successfully');

        return $fileId;
    }

    public function testCreateTestBucket(): string
    {
        $bucketName = self::BUCKET_NAME;
        $bucketId = $this->_client->createBucket($bucketName, 'in', 'Bucket for elastic migration testing');
        $this->assertNotEmpty($bucketId, 'Bucket should be created successfully');

        return $bucketId;
    }

    public function testCreateTestEvent(): array
    {
        $event = new Event();
        $event->setComponent(self::COMPONENT_NAME)
            ->setConfigurationId(self::CONFIG_ID)
            ->setDuration(200)
            ->setType('info')
            ->setRunId('ddddssss')
            ->setMessage('Table Opportunity fetched.')
            ->setDescription('Some longer description of event')
            ->setParams([
                'accountName' => 'Keboola',
                'configuration' => self::CONFIG_ID,
            ]);

        $savedEvent = $this->createAndWaitForEvent($event);

        $assert = function (Event $expected, array $event) {
            $this->assertEquals($expected->getComponent(), $event['component']);
            $this->assertEquals($expected->getConfigurationId(), $event['configurationId']);
            $this->assertEquals($expected->getDuration(), $event['performance']['duration']);
            $this->assertEquals($expected->getType(), $event['type']);
            $this->assertEquals($expected->getRunId(), $event['runId']);
            $this->assertEquals($expected->getMessage(), $event['message']);
            $this->assertEquals($expected->getDescription(), $event['description']);
            $this->assertEquals($expected->getParams(), $event['params']);
            $this->assertGreaterThan(0, $event['idBranch']);
            $this->assertEventUuid($event);
        };
        $assert($event, $savedEvent);

        $savedEvent = $this->_client->getEvent($savedEvent['uuid']);
        $assert($event, $savedEvent);

        return $savedEvent;
    }

    public function testEventSearch()
    {
        // 1. Search by component
        $componentName = self::COMPONENT_NAME;
        $configId = self::CONFIG_ID;
        $events = $this->_client->listEvents([
            'component' => $componentName,
        ]);
        $this->assertGreaterThan(0, count($events), 'Should find events by component');
        
        // Find our specific event by configuration ID
        $foundEvent = null;
        foreach ($events as $event) {
            if ($event['configurationId'] === $configId) {
                $foundEvent = $event;
                break;
            }
        }
        $this->assertNotNull($foundEvent, 'Should find event by configuration ID');
        $this->assertEquals('Table Opportunity fetched.', $foundEvent['message']);
        
        // 2. Search by runId
        $eventsByRunId = $this->_client->listEvents([
            'runId' => 'ddddssss',
        ]);
        $this->assertGreaterThan(0, count($eventsByRunId), 'Should find events by runId');
        $this->assertEquals('ddddssss', $eventsByRunId[0]['runId']);
        
        // 3. Search using Elasticsearch query syntax
        $eventsByQuery = $this->_client->listEvents([
            'q' => 'component:'. $componentName. ' AND configurationId:'. $configId,
        ]);
        $this->assertGreaterThan(0, count($eventsByQuery), 'Should find events by Elasticsearch query');
        $this->assertEquals($componentName, $eventsByQuery[0]['component']);
        $this->assertEquals($configId, $eventsByQuery[0]['configurationId']);
        
        // 4. Search by message content
        $eventsByMessage = $this->_client->listEvents([
            'q' => 'message:"Table Opportunity fetched"',
        ]);
        $this->assertGreaterThan(0, count($eventsByMessage), 'Should find events by message content');
        $this->assertStringContainsString('Table Opportunity fetched', $eventsByMessage[0]['message']);
        
        // 5. Search by params content using more specific query
        $eventsByParams = $this->_client->listEvents([
            'q' => 'component:'. $componentName. ' AND runId:ddddssss AND Keboola',
        ]);
        $this->assertGreaterThan(0, count($eventsByParams), 'Should find events by params content');
        
        // Find the event that contains our specific params
        $foundEventByParams = null;
        foreach ($eventsByParams as $event) {
            if (isset($event['params']['accountName']) && $event['params']['accountName'] === 'Keboola') {
                $foundEventByParams = $event;
                break;
            }
        }
        $this->assertNotNull($foundEventByParams, 'Should find event with specific params content');
        $this->assertEquals('Keboola', $foundEventByParams['params']['accountName']);
    }

    public function testFileSearch()
    {
        // Test list files endpoint comprehensively
        $files = $this->_client->listFiles();
        $this->assertGreaterThan(0, count($files), 'Should have files in the system');
        
        // Test list files with different options
        $listFilesOptions = new \Keboola\StorageApi\Options\ListFilesOptions();
        $listFilesOptions->setLimit(10);
        $filesWithLimit = $this->_client->listFiles($listFilesOptions);
        $this->assertLessThanOrEqual(10, count($filesWithLimit), 'Should respect limit parameter');
        
        // Find our specific file by tag
        $filesByTag = $this->_client->listFiles((new \Keboola\StorageApi\Options\ListFilesOptions())->setTags([self::TAG_NAME]));
        $this->assertGreaterThan(0, count($filesByTag), 'Should find files by tag');
        
        // Verify the file details
        $ourFile = $filesByTag[0];
        $this->assertArrayHasKey('id', $ourFile);
        $this->assertArrayHasKey('name', $ourFile);
        $this->assertArrayHasKey('created', $ourFile);
        $this->assertArrayHasKey('tags', $ourFile);
        $this->assertContains(self::TAG_NAME, $ourFile['tags']);
        
        // Test list files with query (search by filename)
        $filesByQuery = $this->_client->listFiles((new \Keboola\StorageApi\Options\ListFilesOptions())->setQuery('users'));
        $this->assertGreaterThan(0, count($filesByQuery), 'Should find files by query');
        
        // Verify file can be retrieved by ID
        $fileDetails = $this->_client->getFile($ourFile['id']);
        $this->assertEquals($ourFile['id'], $fileDetails['id']);
        $this->assertEquals($ourFile['name'], $fileDetails['name']);
    }

    public function testGlobalSearch()
    {
        // Test global search functionality
        $searchQuery = self::BUCKET_NAME;
        $searchResults = $this->_client->globalSearch($searchQuery);
        
        $this->assertIsArray($searchResults, 'Global search should return an array');
        $this->assertArrayHasKey('all', $searchResults, 'Global search should have "all" count');
        $this->assertArrayHasKey('items', $searchResults, 'Global search should have "items" array');
        $this->assertGreaterThan(0, $searchResults['all'], 'Should find at least one result in global search');
        
        // Find our specific bucket in the search results
        $foundBucket = null;
        foreach ($searchResults['items'] as $item) {
            if ($item['type'] === 'bucket' && strpos($item['name'], self::BUCKET_NAME) !== false) {
                $foundBucket = $item;
                break;
            }
        }
        $this->assertNotNull($foundBucket, 'Should find our bucket in global search results');
        $this->assertEquals('bucket', $foundBucket['type']);
        $this->assertStringContainsString(self::BUCKET_NAME, $foundBucket['name']);
    }
}