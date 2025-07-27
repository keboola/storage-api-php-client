<?php

declare(strict_types=1);

namespace Keboola\StorageApi\Options\TableImport;

enum DeduplicationStrategy: string
{
    case INSERT = 'insert';
    case UPSERT = 'upsert';
}
