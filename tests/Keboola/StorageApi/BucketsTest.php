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

		$inBucketFound = false;
		$outBucketFound = false;
		foreach ($buckets as $bucket) {
			if ($bucket['id'] == 'in.c-main') $inBucketFound = true;
			if ($bucket['id'] == 'out.c-main') $outBucketFound = true;
		}
		$this->assertTrue($inBucketFound);
		$this->assertTrue($outBucketFound);

		$firstBucket = reset($buckets);
		$this->assertArrayHasKey('attributes', $firstBucket);
	}

	public function testBucketsListWithIncludeParameter()
	{
		$buckets = $this->_client->listBuckets(array(
			'include' => '',
		));

		$firstBucket = reset($buckets);
		$this->assertArrayNotHasKey('attributes', $firstBucket);
	}

	public function testBucketDetail()
	{
		$bucket = $this->_client->getBucket('in.c-main');
		$this->assertEquals('mysql', $bucket['backend']);
	}

	public function testBucketCreateWithInvalidBackend()
	{
		try {
			$this->_client->createBucket('unknown-backend', 'in', 'desc', 'redshit');
			$this->fail('Exception should be thrown');
		} catch (\Keboola\StorageApi\ClientException $e){
			$this->assertEquals('storage.buckets.validation', $e->getStringCode());
		}
	}

	/**
	 * @dataProvider backends
	 */
	public function testBucketManipulation($backend)
	{
		$bucketData = array(
			'name' => 'test',
			'stage' => 'in',
			'description' => 'this is just a test',
			'backend' => $backend,
		);
		$newBucketId = $this->_client->createBucket(
			$bucketData['name'],
			$bucketData['stage'],
			$bucketData['description'],
			$bucketData['backend']
		);

		$newBucket = $this->_client->getBucket($newBucketId);
		$this->assertEquals('c-' . $bucketData['name'], $newBucket['name'], 'bucket name');
		$this->assertEquals($bucketData['stage'], $newBucket['stage'], 'bucket stage');
		$this->assertEquals($bucketData['description'], $newBucket['description'], 'bucket description');
		$this->assertEquals($bucketData['backend'], $newBucket['backend'], 'backend');

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
		$this->_client->setBucketAttribute($bucketId, 's', 'lala');
		$this->_client->setBucketAttribute($bucketId, 'other', 'hello', true);
		$bucket = $this->_client->getBucket($bucketId);
		$this->assertArrayEqualsSorted(array(
				array(
					'name' => 's',
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
		$this->_client->setBucketAttribute($bucketId, 's', 'papa');
		$bucket = $this->_client->getBucket($bucketId);
		$this->assertArrayEqualsSorted($bucket['attributes'], array(
			array(
				'name' => 's',
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
		$this->_client->deleteBucketAttribute($bucketId, 's');
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

	public function testBucketExists()
	{
		$this->assertTrue($this->_client->bucketExists('in.c-main'));
		$this->assertFalse($this->_client->bucketExists('in.ukulele'));
	}
}