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
use Keboola\Filter\Exception\InvalidValueProvidedException;
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
    public function testSuccessfullyWebalizeColumnName(string $input, string $expectedOutput): void
    {
        $responseColumnName = $this->_client->webalizeColumnName($input);

        $this->assertSame($expectedOutput, $responseColumnName['columnName']);
    }

    public function validColumnNameData(): Generator
    {
        yield 'currency' => ['currency €', 'currency_EUR'];
        yield 'diacritic' => ['ěĚšŠčČřŘžŽýÝáÁíÍéÉ', 'eEsScCrRzZyYaAiIeE'];
        yield 'special chars' => [
            'lorem &@€\\#˝´˙`˛°˘^ˇ~€||\đĐ[]łŁ}{{@@&##<>*$ß¤×÷¸¨ IPsum',
            'lorem_EUR_EUR_dD_lL_ss_IPsum',
        ];
        yield 'space' => ['muj Bucket', 'muj_Bucket'];
        yield 'reasonable name with special chars' => ['account € & $', 'account_EUR'];
        yield 'invalid chars start and end' => ['$$ some name $$', 'some_name'];
        yield 'leading underscore should be trimmed' => ['_MůjBucketíček', 'MujBucketicek'];
        yield 'long string should be trimmed' => [
            'loremIpsumDolorSitAmetWhateverNextLoremIpsumDolorSitAmetloremIpsumDolorSitAmetWhateverNextLoremIpsumDolorSitAmet',
            'loremIpsumDolorSitAmetWhateverNextLoremIpsumDolorSitAmetloremIps',
        ];
    }

    /**
     * @dataProvider invalidColumnNameData
     */
    public function testFailWebalizeColumnName(
        ?string $input,
        int $expectedErrorCode,
        string $expectedErrorStringCode,
        string $expectedErrorMessage
    ): void {
        try {
            $this->_client->webalizeColumnName($input);
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
            null,
            400,
            'validation.failed',
            'Invalid request:
 - columnName: "This value should not be blank."',
        ];
        yield 'system column' => [
            'oid',
            422,
            'storage.webalize.columnName.invalid',
            'Filtered value "oid" should be valid. \'oid\' is a system column used by the database for internal purposes.',
        ];
        yield 'naughty string 1' => [
            base64_decode('AQIDBAUGBwgODxAREhMUFRYXGBkaGxwdHh9/'),
            422,
            'storage.webalize.columnName.invalid',
            'Filtered value "" should be valid. \'\' contains not allowed characters. Only alphanumeric characters dash and underscores are allowed., \'\' is less than 1 characters long',
        ];
        yield 'only special chars' => [
            '----$$$$-----',
            422,
            'storage.webalize.columnName.invalid',
            'Filtered value "" should be valid. \'\' contains not allowed characters. Only alphanumeric characters dash and underscores are allowed., \'\' is less than 1 characters long',
        ];
    }
}
