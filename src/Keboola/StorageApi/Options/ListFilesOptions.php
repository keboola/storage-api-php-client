<?php
/**
 * Created by JetBrains PhpStorm.
 * User: martinhalamicek
 * Date: 06/02/14
 * Time: 10:43
 * To change this template use File | Settings | File Templates.
 */

namespace Keboola\StorageApi\Options;

class ListFilesOptions
{
    private $tags = [];

    private $limit = 100;

    private $offset = 0;

    private $sinceId;

    private $maxId;

    private $query;

    private $runId;

    /** @var bool */
    private $showExpired = false;

    /**
     * @return array
     */
    public function getTags()
    {
        return $this->tags;
    }

    /**
     * @param array $tags
     * @return $this
     */
    public function setTags(array $tags): static
    {
        $this->tags = $tags;
        return $this;
    }

    /**
     * @return int
     */
    public function getLimit()
    {
        return $this->limit;
    }

    /**
     * @param $limit
     * @return $this
     */
    public function setLimit($limit): static
    {
        $this->limit = (int) $limit;
        return $this;
    }

    /**
     * @return int
     */
    public function getOffset()
    {
        return $this->offset;
    }

    /**
     * @param $offset
     * @return $this
     */
    public function setOffset($offset): static
    {
        $this->offset = (int) $offset;
        return $this;
    }

    public function toArray(): array
    {
        return [
            'limit' => $this->getLimit(),
            'offset' => $this->getOffset(),
            'tags' => $this->getTags(),
            'q' => $this->getQuery(),
            'sinceId' => $this->getSinceId(),
            'maxId' => $this->getMaxId(),
            'runId' => $this->getRunId(),
            'showExpired' => $this->getShowExpired(),
        ];
    }

    /**
     * @return mixed
     */
    public function getQuery()
    {
        return $this->query;
    }

    /**
     * @param $query
     * @return $this
     */
    public function setQuery($query): static
    {
        $this->query = $query;
        return $this;
    }

    /**
     * @return mixed
     */
    public function getSinceId()
    {
        return $this->sinceId;
    }

    /**
     * @param mixed $sinceId
     * @return $this
     */
    public function setSinceId($sinceId): static
    {
        $this->sinceId = (int) $sinceId;
        return $this;
    }

    /**
     * @return mixed
     */
    public function getMaxId()
    {
        return $this->maxId;
    }

    /**
     * @param mixed $maxId
     * @return $this
     */
    public function setMaxId($maxId): static
    {
        $this->maxId = (int) $maxId;
        return $this;
    }

    /**
     * @return mixed
     */
    public function getRunId()
    {
        return $this->runId;
    }

    /**
     * @param mixed $runId
     * @return $this
     */
    public function setRunId($runId): static
    {
        $this->runId = $runId;
        return $this;
    }

    /**
     * @return bool
     */
    public function getShowExpired()
    {
        return $this->showExpired;
    }

    /**
     * @param bool $showExpired
     * @return $this
     */
    public function setShowExpired($showExpired): static
    {
        $this->showExpired = $showExpired;
        return $this;
    }
}
