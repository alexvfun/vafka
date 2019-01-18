package vardsc

import (
	"io/ioutil"
	"log"
	"net/http"
	"net/url"

	yaml "gopkg.in/yaml.v2"
)

//VarnishConf structure for flags
type VarnishConf struct {
	Method   string
	ConfFile string
}

//VarnishCluster - cluster map and purge
type VarnishCluster struct {
	DC      string   `yaml:"DC"`
	Servers []string `yaml:"Servers"`
}

func (vc *VarnishCluster) getYAMLConf(conf VarnishConf) *VarnishCluster {

	yamlFile, err := ioutil.ReadFile(conf.ConfFile)
	if err != nil {
		log.Printf("yamlFile.Get err   #%v ", err)
	}
	err = yaml.Unmarshal(yamlFile, vc)
	if err != nil {
		log.Fatalf("Unmarshal: %v", err)
	}

	return vc
}

// New varnish cluster object
func (vc *VarnishCluster) New(conf VarnishConf) *VarnishCluster {
	switch conf.Method {
	case "conf":
		log.Println("Getting config from ", conf.ConfFile)
		vc.getYAMLConf(conf)
	default:
		vc.DC = "localhost"
		vc.Servers = []string{"10.196.4.8", "10.196.4.9"}
	}

	return vc
}

//Purge for specific url accross varnish cluster.
func (vc *VarnishCluster) Purge(toClean string) error {
	//var logmsg string
	client := &http.Client{}
	req, _ := http.NewRequest("PURGE", "http://"+vc.Servers[0], nil)
	req.Host = toClean
	for _, server := range vc.Servers {
		req.URL, _ = url.Parse("http://" + server)
		resp, err := client.Do(req)
		if err != nil {
			log.Println(err)
			return err
		}
		defer resp.Body.Close()

		// Read Response Body
		respBody, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Println(err)
			return err
		}
		log.Printf("Got response from Varnish: %s:\n\n,%s", server, string(respBody))
	}
	return nil

}
