<?php

namespace Keboola\Test\Downloader;

use Keboola\StorageApi\Downloader\AbsUrlParser;

class AbsUrlParserTest extends \PHPUnit\Framework\TestCase
{
    public function testParseAbsUrlAzure()
    {
        $url = 'azure://myaccount.blob.core.windows.net/mycontainer/myblob';
        list($protocol, $account, $container, $file) = AbsUrlParser::parseAbsUrl($url);

        $this->assertEquals('azure', $protocol);
        $this->assertEquals('myaccount', $account);
        $this->assertEquals('mycontainer', $container);
        $this->assertEquals('myblob', $file);
    }

    public function testParseAbsUrlHttps()
    {
        $url = 'https://myaccount.blob.core.windows.net/mycontainer/myblob';
        list($protocol, $account, $container, $file) = AbsUrlParser::parseAbsUrl($url);

        $this->assertEquals('https', $protocol);
        $this->assertEquals('myaccount', $account);
        $this->assertEquals('mycontainer', $container);
        $this->assertEquals('myblob', $file);
    }
}
