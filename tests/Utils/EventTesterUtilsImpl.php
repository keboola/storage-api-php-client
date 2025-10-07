<?php

declare(strict_types=1);

namespace Keboola\Test\Utils;

use Keboola\StorageApi\Client;
use PHPUnit\Framework\TestCase;

/**
 * @property Client $_client
 */
class EventTesterUtilsImpl extends TestCase
{
    use EventTesterUtils;

    public function __construct()
    {
        parent::__construct();
        $this->tokenId = '7';
        $this->lastEventId['test'] = '10';
    }
}
