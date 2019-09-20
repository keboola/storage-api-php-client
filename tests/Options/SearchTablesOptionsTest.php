<?php

namespace Keboola\Test\Options;

use Keboola\StorageApi\Options\SearchTablesOptions;
use Keboola\Test\StorageApiTestCase;

class SearchTablesOptionsTest extends StorageApiTestCase
{
    public function testGetDefaults()
    {
        $options = new SearchTablesOptions();
        $this->assertEquals(null, $options->getMetadataKey());
        $this->assertEquals(null, $options->getMetadataValue());
        $this->assertEquals(null, $options->getMetadataProvider());
    }

    public function testCreate()
    {
        $options = SearchTablesOptions::create(null, null, null);
        $this->assertInstanceOf(SearchTablesOptions::class, $options);
        $this->assertEquals(null, $options->getMetadataKey());
        $this->assertEquals(null, $options->getMetadataValue());
        $this->assertEquals(null, $options->getMetadataProvider());

        $this->expectException(\Exception::class);
        $this->expectExceptionMessage('At least one option must be set');
        $options->validate();

        $options = SearchTablesOptions::create('key', 'value', 'provider');
        $this->assertInstanceOf(SearchTablesOptions::class, $options);
        $this->assertEquals('key', $options->getMetadataKey());
        $this->assertEquals('value', $options->getMetadataValue());
        $this->assertEquals('provider', $options->getMetadataProvider());

        $options->validate(); // should not throw exception

        $this->assertSame([
            'metadataKey' => 'key',
            'metadataValue' => 'value',
            'metadataProvider' => 'provider',
        ], $options->toArray());
    }

    public function testFluentInterface()
    {
        $options = new SearchTablesOptions();
        $this->assertInstanceOf(SearchTablesOptions::class, $options->setMetadataKey('key'));
        $this->assertEquals('key', $options->getMetadataKey());
        $this->assertInstanceOf(SearchTablesOptions::class, $options->setMetadataValue('value'));
        $this->assertEquals('value', $options->getMetadataValue());
        $this->assertInstanceOf(SearchTablesOptions::class, $options->setMetadataProvider('provider'));
        $this->assertEquals('provider', $options->getMetadataProvider());
    }
}
