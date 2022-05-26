<?php
/**
 * Created by JetBrains PhpStorm.
 * User: martinhalamicek
 * Date: 22/05/14
 * Time: 16:35
 * To change this template use File | Settings | File Templates.
 */

namespace Keboola\Test\Common;

use Keboola\StorageApi\Client;
use Keboola\Test\StorageApiTestCase;

class CommonTest extends StorageApiTestCase
{

    public function testParseCsv(): void
    {
        $csvData = '"column1","column2"' . PHP_EOL
            . '"valu\ "",e1","value2"' . PHP_EOL
            . '"new' . PHP_EOL . 'line","col2"';

        $expectedSimple = [
            [
                "column1",
                "column2",
            ],
            [
                'valu\ ",e1', 'value2',
            ],
            [
                "new\nline", "col2",
            ],
        ];
        $expectedHashmap = [
            [
                "column1" => 'valu\ ",e1',
                "column2" => 'value2',
            ],
            [
                "column1" => "new\nline",
                "column2" => "col2",
            ],
        ];

        $data = \Keboola\StorageApi\Client::parseCsv($csvData, false);
        $this->assertEquals($expectedSimple, $data, "Csv parse to flat array");

        $data = \Keboola\StorageApi\Client::parseCsv($csvData, true);
        $this->assertEquals($expectedHashmap, $data, "Csv parse to associative array");
    }

    public function testUrlShouldBeRequired(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        new Client([
            'token' => STORAGE_API_TOKEN,
        ]);
    }

    public function testAwsRetries(): void
    {
        $retriesCount = 234;
        $client = $this->getClient([
            'token' => STORAGE_API_TOKEN,
            'url' => STORAGE_API_URL,
            'backoffMaxTries' => 1,
            'awsRetries' => $retriesCount,
        ]);
        $this->assertEquals($retriesCount, $client->getAwsRetries());
    }
}
