<?php
/**
 *
 * User: Martin Halamíček
 * Date: 16.5.12
 * Time: 11:46
 *
 */

namespace Keboola\Test\Common;

use Generator;
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

    /**
     * @dataProvider validColumnNameData
     */
    public function testSuccessfullyWebalizeColumnName(array $input, array $expectedOutput): void
    {
        $responseColumnNames = $this->_client->webalizeColumnNames($input);

        $this->assertSame($expectedOutput, $responseColumnNames['columnNames']);
    }

    public function validColumnNameData(): Generator
    {
        yield 'all' => [
            [
                'currency €',
                'ěĚšŠčČřŘžŽýÝáÁíÍéÉ',
                'lorem &@€\\#˝´˙`˛°˘^ˇ~€||\đĐ[]łŁ}{{@@&##<>*$ß¤×÷¸¨ IPsum',
                'muj Bucket',
                'account € & $',
                '$$ some name $$',
                '_MůjBucketíček',
                'loremIpsumDolorSitAmetWhateverNextLoremIpsumDolorSitAmetloremIpsumDolorSitAmetWhateverNextLoremIpsumDolorSitAmet',
                'oid',
                base64_decode('AQIDBAUGBwgODxAREhMUFRYXGBkaGxwdHh9/'),
                '----$$$$-----',
            ],
            [
                'currency_EUR',
                'eEsScCrRzZyYaAiIeE',
                'lorem_EUR_EUR_dD_lL_ss_IPsum',
                'muj_Bucket',
                'account_EUR',
                'some_name',
                'MujBucketicek',
                'loremIpsumDolorSitAmetWhateverNextLoremIpsumDolorSitAmetloremIps',
                'oid',
                '',
                '',
            ],
        ];
    }

    /**
     * @dataProvider validWebalizedColumnNameData
     */
    public function testSuccessfullyValidateColumnNames(array $webalizedInput): void
    {
        $response = $this->_client->validateColumnNames($webalizedInput);
        $this->assertEmpty($response, 'When empty, everything is validated.');
    }

    public function validWebalizedColumnNameData(): Generator
    {
        yield 'all' => [
            [
                'currency_EUR',
                'eEsScCrRzZyYaAiIeE',
                'lorem_EUR_EUR_dD_lL_ss_IPsum',
                'muj_Bucket',
                'account_EUR',
                'some_name',
                'MujBucketicek',
                'loremIpsumDolorSitAmetWhateverNextLoremIpsumDolorSitAmetloremIps',
            ],
        ];
    }

    /**
     * @dataProvider invalidWebalizedColumnNameData
     */
    public function testInvalidColumnNames(array $columnNames): void
    {
        try {
            $this->_client->validateColumnNames($columnNames);
            $this->fail('Invalid columns should fail');
        } catch (ClientException $exception) {
            $expectedErrorMessages = [];
            foreach ($columnNames as $columnName) {
                $expectedErrorMessages[] = "\"{$columnName}\" is a system column used by the database for internal purposes.";
            }
            $errorMessages = $exception->getContextParams();
            $this->assertEquals($expectedErrorMessages, $errorMessages);
        }
    }

    public function invalidWebalizedColumnNameData(): Generator
    {
        yield 'all' => [
            [
                'oid',
                'tableoid',
                'xmin',
                'cmin',
                'xmax',
                'cmax',
                'ctid',
            ],
        ];
    }

    /**
     * @dataProvider invalidColumnNameData
     */
    public function testFailWebalizeColumnName(
        array $input,
        int $expectedErrorCode,
        string $expectedErrorStringCode,
        string $expectedErrorMessage
    ): void {
        try {
            $this->_client->webalizeColumnNames($input);
            $this->fail('fail webalize columnName');
        } catch (ClientException $e) {
            $this->assertEquals($expectedErrorMessage, $e->getMessage());
            $this->assertEquals($expectedErrorStringCode, $e->getStringCode());
            $this->assertEquals($expectedErrorCode, $e->getCode());
        }
    }

    public function invalidColumnNameData(): Generator
    {
        yield 'null' => [
            [null],
            400,
            'validation.failed',
            'Invalid request:
 - columnNames[0]: "This value should not be blank."',
        ];
    }
}
