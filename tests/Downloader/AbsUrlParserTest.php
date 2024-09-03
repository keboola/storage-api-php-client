<?php

namespace Keboola\Test\Downloader;

use InvalidArgumentException;
use Keboola\StorageApi\Downloader\AbsUrlParser;
use PHPUnit\Framework\TestCase;

class AbsUrlParserTest extends TestCase
{
    public function testParseAbsUrlAzure(): void
    {
        $url = 'azure://myaccount.blob.core.windows.net/mycontainer/myblob';
        list($protocol, $account, $container, $file) = AbsUrlParser::parseAbsUrl($url);

        $this->assertEquals('azure', $protocol);
        $this->assertEquals('myaccount', $account);
        $this->assertEquals('mycontainer', $container);
        $this->assertEquals('myblob', $file);
    }

    public function testParseAbsUrlHttps(): void
    {
        $url = 'https://myaccount.blob.core.windows.net/mycontainer/myblob';
        list($protocol, $account, $container, $file) = AbsUrlParser::parseAbsUrl($url);

        $this->assertEquals('https', $protocol);
        $this->assertEquals('myaccount', $account);
        $this->assertEquals('mycontainer', $container);
        $this->assertEquals('myblob', $file);
    }

    public function testException(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('The provided URL does not match the expected format.');
        AbsUrlParser::parseAbsUrl('invalid-url');
    }
}
