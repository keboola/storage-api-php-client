<?php

namespace Keboola\Test\S3Uploader;

use Keboola\StorageApi\S3Uploader\Chunker;
use Keboola\Test\StorageApiTestCase;

class ChunkerTest extends StorageApiTestCase
{

    public function testSettersAndGetters()
    {
        $chunker = new Chunker();
        $this->assertEquals(50, $chunker->getSize());
        $chunker->setSize(10);
        $this->assertEquals(10, $chunker->getSize());
        $chunker = new Chunker(5);
        $this->assertEquals(5, $chunker->getSize());
    }

    public function testSimpleChunker()
    {
        $chunker = new Chunker(3);
        $items = [1, 2, 3, 4, 5];
        $expected = [[1, 2, 3], [4, 5]];
        $this->assertEquals($expected, $chunker->makeChunks($items));
    }

    public function testAssociativeArrayChunker()
    {
        $chunker = new Chunker(3);
        $items = [
            "item1" => 1,
            "item2" => 2,
            "item3" => 3,
            "item4" => 4,
            "item5" => 5
        ];
        $expected = [
            [
                "item1" => 1,
                "item2" => 2,
                "item3" => 3
            ],
            [
                "item4" => 4,
                "item5" => 5
            ]
        ];
        $this->assertEquals($expected, $chunker->makeChunks($items));
    }
}
