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
use const PHP_EOL;

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
     * @dataProvider validColumnNameDataForValidation
     */
    public function testSuccessfullyValidateColumnNames(array $webalizedInput): void
    {
        $response = $this->_client->validateColumnNames($webalizedInput);
        $this->assertEmpty($response, 'When empty, everything is validated.');
    }

    public function validColumnNameDataForValidation(): Generator
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
     * @dataProvider invalidColumnNameDataForValidation
     */
    public function testInvalidColumnNames(array $columnNames, array $validationErrorMessages): void
    {
        try {
            $this->_client->validateColumnNames($columnNames);
            $this->fail('Invalid columns should fail');
        } catch (ClientException $exception) {
            $expectedMessages = [];
            $expectedErrorMessages = [];
            foreach ($validationErrorMessages as $key => $errorMessage) {
                $keyName = sprintf('columnNames[%d]', $key);
                $expectedMessages[] = sprintf(' - %s: "%s"', $keyName, $errorMessage);
                $expectedErrorMessages[] = [
                    'key' => $keyName,
                    'message' => $errorMessage,
                ];
            }
            $expectedMessage = 'Invalid request:' . PHP_EOL . implode(PHP_EOL, $expectedMessages);
            $this->assertEquals($expectedMessage, $exception->getMessage());
            $this->assertEquals($expectedErrorMessages, $exception->getContextParams()['errors']);
        }
    }

    public function invalidColumnNameDataForValidation(): Generator
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
                'currency €',
                'ěĚšŠčČřŘžŽýÝáÁíÍéÉ',
                'lorem &@€\\#˝´˙`˛°˘^ˇ~€||\đĐ[]łŁ}{{@@&##<>*$ß¤×÷¸¨ IPsum',
                'muj Bucket',
                'account € & $',
                '$$ some name $$',
                '_MůjBucketíček',
                'loremIpsumDolorSitAmetWhateverNextLoremIpsumDolorSitAmetloremIpsumDolorSitAmetWhateverNextLoremIpsumDolorSitAmet',
                base64_decode('AQIDBAUGBwgODxAREhMUFRYXGBkaGxwdHh9/'),
                '----$$$$-----',
            ],
            [
                '"oid" is a system column used by the database for internal purposes.',
                '"tableoid" is a system column used by the database for internal purposes.',
                '"xmin" is a system column used by the database for internal purposes.',
                '"cmin" is a system column used by the database for internal purposes.',
                '"xmax" is a system column used by the database for internal purposes.',
                '"cmax" is a system column used by the database for internal purposes.',
                '"ctid" is a system column used by the database for internal purposes.',
                '"currency €" contains not allowed characters. Only alphanumeric characters dash and underscores are allowed.',
                '"ěĚšŠčČřŘžŽýÝáÁíÍéÉ" contains not allowed characters. Only alphanumeric characters dash and underscores are allowed.',
                '"lorem &@€\#˝´˙`˛°˘^ˇ~€||\đĐ[]łŁ}{{@@&##<>*$ß¤×÷¸¨ IPsum" contains not allowed characters. Only alphanumeric characters dash and underscores are allowed.',
                '"muj Bucket" contains not allowed characters. Only alphanumeric characters dash and underscores are allowed.',
                '"account € & $" contains not allowed characters. Only alphanumeric characters dash and underscores are allowed.',
                '"$$ some name $$" contains not allowed characters. Only alphanumeric characters dash and underscores are allowed.',
                '"_MůjBucketíček" contains not allowed characters. Only alphanumeric characters dash and underscores are allowed.',
                'Column name cannot be longer than 64 characters.',
                '"'.base64_decode('AQIDBAUGBwgODxAREhMUFRYXGBkaGxwdHh9/').'" contains not allowed characters. Only alphanumeric characters dash and underscores are allowed.',
                '"----$$$$-----" contains not allowed characters. Only alphanumeric characters dash and underscores are allowed.',
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
