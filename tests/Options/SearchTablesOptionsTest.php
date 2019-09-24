<?php

namespace Keboola\Test\Options;

use Keboola\StorageApi\Options\SearchTablesOptions;
use Keboola\Test\StorageApiTestCase;

class SearchTablesOptionsTest extends StorageApiTestCase
{
    public function testGetDefaults()
    {
        $options = new SearchTablesOptions();
        $this->assertSame([
            'metadataKey' => null,
            'metadataValue' => null,
            'metadataProvider' => null,
        ], $options->toArray());
    }

    public function testCreate()
    {
        $options = SearchTablesOptions::create(null, null, null);
        $this->assertInstanceOf(SearchTablesOptions::class, $options);
        $this->assertSame([
            'metadataKey' => null,
            'metadataValue' => null,
            'metadataProvider' => null,
        ], $options->toArray());

        $options = SearchTablesOptions::create('key', 'value', 'provider');
        $this->assertInstanceOf(SearchTablesOptions::class, $options);

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
        $this->assertInstanceOf(SearchTablesOptions::class, $options->setMetadataValue('value'));
        $this->assertInstanceOf(SearchTablesOptions::class, $options->setMetadataProvider('provider'));

        $this->assertSame([
            'metadataKey' => 'key',
            'metadataValue' => 'value',
            'metadataProvider' => 'provider',
        ], $options->toArray());
    }
}
