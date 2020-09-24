<?php

namespace Keboola\Test;

use Keboola\StorageApi\Client;
use PHPUnit\Framework\TestCase;

class ClientTestCase extends TestCase
{
    public function getClient(array $options)
    {
        $buildId = '';
        if (getenv('TRAVIS_BUILD_ID')) {
            $buildId = sprintf('Build id: ' . getenv('TRAVIS_BUILD_ID'));
        }

        $options['userAgent'] = sprintf('%s Test: %s\\%s', $buildId, get_class($this), $this->getName());
        return new Client($options);
    }
}
