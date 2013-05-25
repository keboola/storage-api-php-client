<?php
/**
 *
 * User: Martin Halamíček
 * Date: 16.5.12
 * Time: 11:46
 *
 */

class Keboola_StorageApi_BucketsTest extends StorageApiTestCase
{

	public function testBucketsList()
	{
		$buckets = $this->_client->listBuckets();
		$this->assertTrue(count($buckets) >= 2);

		$this->assertEquals('in.c-main', $buckets[0]['id']);
		$this->assertEquals('out.c-main', $buckets[1]['id']);
	}

	public function testBucketDetail()
	{
		$bucket = $this->_client->getBucket('in.c-main');
		$this->assertEquals('in.c-main', $bucket['id']);
	}

	public function testBucketManipulation()
	{
		$bucketData = array(
			'name' => 'test',
			'stage' => 'in',
			'description' => 'this is just a test',
		);
		$newBucketId = $this->_client->createBucket($bucketData['name'], $bucketData['stage'], $bucketData['description']);


		$newBucket = $this->_client->getBucket($newBucketId);
		$this->assertEquals('c-' . $bucketData['name'], $newBucket['name'], 'bucket name');
		$this->assertEquals($bucketData['stage'], $newBucket['stage'], 'bucket stage');
		$this->assertEquals($bucketData['description'], $newBucket['description'], 'bucket description');

		// check if bucket is in list
		$buckets = $this->_client->listBuckets();
		$this->assertTrue(in_array($newBucketId, array_map(function($bucket) {
			return $bucket['id'];
		}, $buckets)));

		$this->_client->dropBucket($newBucket['id']);
	}

	public function testBucketAttributes()
	{
		$bucketId = 'in.c-main';

		$bucket = $this->_client->getBucket($bucketId);
		$this->assertEmpty($bucket['attributes'], 'empty attributes');


		// create
		$this->_client->setBucketAttribute($bucketId, 'something', 'lala');
		$this->_client->setBucketAttribute($bucketId, 'other', 'hello', true);
		$bucket = $this->_client->getBucket($bucketId);
		$this->assertArrayEqualsSorted(array(
				array(
					'name' => 'something',
					'value' => 'lala',
					'protected' => false,
				),
				array(
					'name' => 'other',
					'value' => 'hello',
					'protected' => true,
				),
			), $bucket['attributes'], 'name', 'attribute set');

		// update
		$this->_client->setBucketAttribute($bucketId, 'something', 'papa');
		$bucket = $this->_client->getBucket($bucketId);
		$this->assertArrayEqualsSorted($bucket['attributes'], array(
			array(
				'name' => 'something',
				'value' => 'papa',
				'protected' => false,
			),
			array(
				'name' => 'other',
				'value' => 'hello',
				'protected' => true,
			),
		), 'name', 'attribute update');

		// delete
		$this->_client->deleteBucketAttribute($bucketId, 'something');
		$bucket = $this->_client->getBucket($bucketId);
		$this->assertArrayEqualsSorted($bucket['attributes'], array(
			array(
				'name' => 'other',
				'value' => 'hello',
				'protected' => true,
			)
		), 'name', 'attribute delete');

		$this->_client->deleteBucketAttribute($bucketId, 'other');
	}

}