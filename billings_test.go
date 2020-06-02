package billings_test

import (
	"log"
	"math/rand"
	"testing"

	"github.com/cbi-sh/billings"
)

func Test(t *testing.T) {
	billings.SelfTest()
}

func BenchmarkGet(b *testing.B) {
	b.ResetTimer()
	for msisdn := int64(0); msisdn < int64(b.N); msisdn++ {
		if _, err := billings.Get(msisdn); err != nil {
			log.Fatal(err)
		}
	}
}

func BenchmarkSet(b *testing.B) {
	b.ResetTimer()
	for msisdn := int64(0); msisdn < int64(b.N); msisdn++ {
		if err := billings.Set(msisdn, int8(rand.Int31n(3))); err != nil {
			log.Fatal(err)
		}
	}
}
