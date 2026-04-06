#!/usr/bin/env python3
"""
Flashlight Product Scraper for Sofirn and Wurkkos
Extracts product information including model, LED, throw distance, etc.
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import time
from urllib.parse import urljoin
import json


class FlashlightScraper:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        self.products = []
    
    def scrape_sofirn(self, base_url='https://www.sofirnlight.com'):
        """Scrape Sofirn product listings"""
        print("Scraping Sofirn...")
        
        # Common Sofirn collection URLs
        collection_urls = [
            f'{base_url}/collections/all-products',
            f'{base_url}/collections/flashlights',
            f'{base_url}/collections/headlamps'
        ]
        
        for url in collection_urls:
            try:
                print(f"Fetching: {url}")
                response = requests.get(url, headers=self.headers, timeout=10)
                response.raise_for_status()
                
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Find product links (adjust selectors based on actual site structure)
                product_links = soup.find_all('a', href=re.compile(r'/products/'))
                
                for link in product_links:
                    product_url = urljoin(base_url, link['href'])
                    if product_url not in [p['url'] for p in self.products]:
                        product_data = self.scrape_product_page(product_url, 'Sofirn')
                        if product_data:
                            self.products.append(product_data)
                            time.sleep(1)  # Be polite with scraping
                
            except Exception as e:
                print(f"Error scraping {url}: {e}")
    
    def scrape_wurkkos(self, base_url='https://www.wurkkos.com'):
        """Scrape Wurkkos product listings"""
        print("Scraping Wurkkos...")
        
        # Common Wurkkos collection URLs
        collection_urls = [
            f'{base_url}/collections/all-products',
            f'{base_url}/collections/flashlights',
            f'{base_url}/collections/headlamps'
        ]
        
        for url in collection_urls:
            try:
                print(f"Fetching: {url}")
                response = requests.get(url, headers=self.headers, timeout=10)
                response.raise_for_status()
                
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Find product links
                product_links = soup.find_all('a', href=re.compile(r'/products/'))
                
                for link in product_links:
                    product_url = urljoin(base_url, link['href'])
                    if product_url not in [p['url'] for p in self.products]:
                        product_data = self.scrape_product_page(product_url, 'Wurkkos')
                        if product_data:
                            self.products.append(product_data)
                            time.sleep(1)  # Be polite with scraping
                
            except Exception as e:
                print(f"Error scraping {url}: {e}")
    
    def scrape_product_page(self, url, brand):
        """Scrape individual product page for details"""
        try:
            print(f"  Scraping product: {url}")
            response = requests.get(url, headers=self.headers, timeout=10)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extract product information
            product_data = {
                'brand': brand,
                'url': url,
                'model': self.extract_model(soup, url),
                'name': self.extract_name(soup),
                'price': self.extract_price(soup),
                'led': self.extract_led(soup),
                'throw': self.extract_throw(soup),
                'lumens': self.extract_lumens(soup),
                'battery': self.extract_battery(soup),
                'description': self.extract_description(soup)
            }
            
            return product_data
            
        except Exception as e:
            print(f"  Error scraping product page {url}: {e}")
            return None
    
    def extract_model(self, soup, url):
        """Extract model number from page"""
        # Try to find in title
        title = soup.find('h1')
        if title:
            text = title.get_text()
            # Match patterns like "IF19", "SP33", "TS21", etc.
            match = re.search(r'\b([A-Z]{1,3}\d{1,3}[A-Z]?)\b', text)
            if match:
                return match.group(1)
        
        # Try URL
        match = re.search(r'/([a-z]{1,3}\d{1,3}[a-z]?)', url, re.IGNORECASE)
        if match:
            return match.group(1).upper()
        
        return None
    
    def extract_name(self, soup):
        """Extract product name"""
        title = soup.find('h1')
        return title.get_text().strip() if title else None
    
    def extract_price(self, soup):
        """Extract price"""
        # Common price selectors
        price_selectors = [
            {'class_': 'price'},
            {'class_': 'product-price'},
            {'itemprop': 'price'}
        ]
        
        for selector in price_selectors:
            price = soup.find(['span', 'div'], selector)
            if price:
                text = price.get_text().strip()
                # Extract numeric value
                match = re.search(r'\$?(\d+\.?\d*)', text)
                if match:
                    return float(match.group(1))
        
        return None
    
    def extract_led(self, soup):
        """Extract LED type"""
        text = soup.get_text()
        
        # Common LED patterns
        led_patterns = [
            r'(CREE\s+XHP\d+(?:\.\d)?)',
            r'(XHP\d+(?:\.\d)?)',
            r'(XPL(?:-?HI)?)',
            r'(SST\d+)',
            r'(Luminus\s+SFT\d+)',
            r'(SFT\d+)',
            r'(Osram\s+[A-Z0-9]+)',
            r'(Samsung\s+LH351D)',
            r'(LH351D)',
            r'(Nichia\s+\d+)'
        ]
        
        for pattern in led_patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return match.group(1)
        
        return None
    
    def extract_throw(self, soup):
        """Extract throw distance in meters"""
        text = soup.get_text()
        
        # Match patterns like "650m", "650 meters", "650 m throw"
        patterns = [
            r'(\d+)\s*(?:meters?|m)\s+(?:throw|range)',
            r'throw[:\s]+(\d+)\s*(?:meters?|m)',
            r'(\d+)\s*m\b'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return int(match.group(1))
        
        return None
    
    def extract_lumens(self, soup):
        """Extract maximum lumens"""
        text = soup.get_text()
        
        # Match patterns like "6000 lumens", "6000lm"
        patterns = [
            r'(\d+)\s*(?:lumens?|lm)',
            r'(\d+)\s*lm\b'
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            if matches:
                # Return the maximum value found
                return max([int(m) for m in matches])
        
        return None
    
    def extract_battery(self, soup):
        """Extract battery type"""
        text = soup.get_text()
        
        # Common battery patterns
        patterns = [
            r'(\d+x?\s*\d*\s*(?:18650|21700|26650|AAA?|14500))',
            r'(18650|21700|26650)',
            r'(USB-C\s+rechargeable)',
            r'(built-in\s+battery)'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return match.group(1)
        
        return None
    
    def extract_description(self, soup):
        """Extract product description"""
        desc_selectors = [
            {'class_': 'product-description'},
            {'class_': 'description'},
            {'itemprop': 'description'}
        ]
        
        for selector in desc_selectors:
            desc = soup.find(['div', 'p'], selector)
            if desc:
                return desc.get_text().strip()[:500]  # Limit length
        
        return None
    
    def save_to_csv(self, filename='flashlight_products.csv'):
        """Save scraped data to CSV"""
        if not self.products:
            print("No products to save!")
            return
        
        df = pd.DataFrame(self.products)
        df.to_csv(filename, index=False)
        print(f"\nSaved {len(self.products)} products to {filename}")
        
        # Print summary statistics
        print("\n=== Summary Statistics ===")
        print(f"Total products: {len(self.products)}")
        print(f"Sofirn products: {len(df[df['brand'] == 'Sofirn'])}")
        print(f"Wurkkos products: {len(df[df['brand'] == 'Wurkkos'])}")
        print(f"\nProducts with LED info: {df['led'].notna().sum()}")
        print(f"Products with throw info: {df['throw'].notna().sum()}")
        print(f"Products with lumens info: {df['lumens'].notna().sum()}")
    
    def save_to_json(self, filename='flashlight_products.json'):
        """Save scraped data to JSON"""
        if not self.products:
            print("No products to save!")
            return
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(self.products, f, indent=2, ensure_ascii=False)
        print(f"Saved {len(self.products)} products to {filename}")


def main():
    scraper = FlashlightScraper()
    
    print("=" * 60)
    print("Flashlight Product Scraper")
    print("=" * 60)
    
    # Scrape both sites
    scraper.scrape_sofirn()
    print("\n" + "=" * 60 + "\n")
    scraper.scrape_wurkkos()
    
    # Save results
    print("\n" + "=" * 60)
    scraper.save_to_csv('flashlight_products.csv')
    scraper.save_to_json('flashlight_products.json')
    
    # Display sample
    if scraper.products:
        print("\n=== Sample Products ===")
        df = pd.DataFrame(scraper.products)
        print(df[['brand', 'model', 'name', 'led', 'throw', 'lumens']].head(10))


if __name__ == '__main__':
    main()