<?php
/**
 *
 * User: Martin Halamíček
 * Date: 16.5.12
 * Time: 11:46
 *
 */

namespace Keboola\Test\Common;

use Keboola\StorageApi\ClientException;
use Keboola\StorageApi\Options\IndexOptions;
use Keboola\Test\StorageApiTestCase;

class IndexTest extends StorageApiTestCase
{

    public function testIndex(): void
    {
        $index = $this->_client->indexAction();
        $this->assertEquals('storage', $index['api']);
        $this->assertEquals('v2', $index['version']);
        $this->assertArrayHasKey('revision', $index);

        $this->assertArrayHasKey('stack', $index);
        $this->assertIsString($index['stack']);

        $this->assertIsArray($index['components']);
        $this->assertIsArray($index['features']);

        $component = reset($index['components']);
        $this->assertArrayHasKey('id', $component);
        $this->assertArrayHasKey('uri', $component);

        $this->assertArrayHasKey('urlTemplates', $index);

        $urlTemplates = $index['urlTemplates'];
        $this->assertArrayHasKey('orchestrationJob', $urlTemplates);
    }

    public function testIndexExclude(): void
    {
        // exclude=components
        $index = $this->_client->indexAction((new IndexOptions())->setExclude(['components']));
        $this->assertEquals('storage', $index['api']);
        $this->assertEquals('v2', $index['version']);
        $this->assertArrayHasKey('revision', $index);

        $this->assertArrayNotHasKey('components', $index);
        $this->assertIsArray($index['features']);

        $this->assertArrayHasKey('urlTemplates', $index);

        $urlTemplates = $index['urlTemplates'];
        $this->assertArrayHasKey('orchestrationJob', $urlTemplates);

        // exclude=componentDetails
        $indexWithoutComponentDetails = $this->_client->indexAction((new IndexOptions())->setExclude(['componentDetails']));
        $this->assertArrayHasKey('components', $indexWithoutComponentDetails);

        $componentsWithoutDetails = $indexWithoutComponentDetails['components'];
        $this->assertArrayHasKey(0, $componentsWithoutDetails);

        $firstComponent = $componentsWithoutDetails[0];
        $this->assertArrayHasKey('id', $firstComponent);
        $this->assertArrayHasKey('name', $firstComponent);
        $this->assertArrayHasKey('type', $firstComponent);
        $this->assertArrayHasKey('ico64', $firstComponent);
        $this->assertArrayHasKey('ico128', $firstComponent);
        $this->assertArrayHasKey('description', $firstComponent);
        $this->assertArrayHasKey('features', $firstComponent);
        $this->assertArrayHasKey('flags', $firstComponent);
        $this->assertArrayHasKey('categories', $firstComponent);

        // exclude=components,componentDetails
        $indexWithoutComponents = $this->_client->indexAction((new IndexOptions())->setExclude(['components', 'componentDetails']));
        $this->assertArrayNotHasKey('components', $indexWithoutComponents);
    }

    public function testSuccessfullyWebalizeDisplayName(): void
    {
        $responseDisplayName = $this->_client->webalizeDisplayName('Môj 1$ obľúbený bucket $');

        $this->assertSame('Moj-1-oblubeny-bucket', $responseDisplayName['displayName']);
    }

    public function testFailWebalizeDisplayName(): void
    {
        try {
            $this->_client->webalizeDisplayName('-----');
            $this->fail('fail webalize displayName');
        } catch (ClientException $e) {
            $this->assertEquals('Filtered name "" should be valid name', $e->getMessage());
            $this->assertEquals('storage.webalize.displayName.invalid', $e->getStringCode());
            $this->assertEquals(422, $e->getCode());
        }
    }
}
